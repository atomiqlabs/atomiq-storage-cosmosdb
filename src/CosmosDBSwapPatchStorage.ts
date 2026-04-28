import {
    CompositePath,
    DeleteOperationInput,
    IndexedPath,
    IndexingPolicy, PatchOperationInput,
    ReplaceOperationInput,
    UpsertOperationInput
} from "@azure/cosmos";
import {
    IUnifiedStorage,
    QueryParams,
    UnifiedStoredObject,
    UnifiedSwapStorageCompositeIndexes,
    UnifiedSwapStorageIndexes
} from "@atomiqlabs/sdk";
import {
    CosmosDBBase,
    isNotFoundStatusCode,
    isPreconditionFailedStatusCode,
    parseStatusCode
} from "./CosmosDBBase";
import {CosmosDBConcurrencyError} from "./CosmosDBConcurrencyError";
import {calculatePatch, deepClone} from "./Utils";

type DeleteOperationInputWithIfMatch = DeleteOperationInput & {
    ifMatch?: string;
};

function toCosmosPathSegment(value: string): string {
    return /^[A-Za-z0-9_]+$/.test(value) ? value : JSON.stringify(value);
}

function toScalarPath(value: string): string {
    return "/" + toCosmosPathSegment(value) + "/?";
}

function toCompositeScalarPath(value: string): string {
    return "/" + toCosmosPathSegment(value);
}

function buildIndexingPolicy(
    indexes: UnifiedSwapStorageIndexes,
    compositeIndexes: UnifiedSwapStorageCompositeIndexes
): IndexingPolicy {
    const includedPathsMap = new Map<string, IndexedPath>();

    indexes.forEach(index => {
        if(index.key==="id") return;
        includedPathsMap.set(toScalarPath(index.key), {
            path: toScalarPath(index.key)
        });
    });

    compositeIndexes.forEach(index => {
        index.keys.forEach(key => {
            if(key==="id") return;
            const path = toScalarPath(key);
            if(includedPathsMap.has(path)) return;
            includedPathsMap.set(path, {path});
        });
    });

    const cosmosCompositeIndexes: CompositePath[][] = compositeIndexes
        .filter(index => index.keys.length > 1)
        .map(index => index.keys.map(key => ({
            path: toCompositeScalarPath(key),
            order: "ascending"
        })));

    return {
        automatic: true,
        indexingMode: "consistent",
        includedPaths: Array.from(includedPathsMap.values()),
        excludedPaths: [
            {path: "/*"}
        ],
        compositeIndexes: cosmosCompositeIndexes
    };
}

export class CosmosDBSwapPatchStorage extends CosmosDBBase implements IUnifiedStorage<UnifiedSwapStorageIndexes, UnifiedSwapStorageCompositeIndexes> {

    optimisticConcurrency: boolean;

    constructor(chainId: string, connectionString: string, databaseName: string = "Atomiq", optimisticConcurrency: boolean = false) {
        super(chainId, connectionString, databaseName);
        this.optimisticConcurrency = optimisticConcurrency;
    }

    async init(indexes: UnifiedSwapStorageIndexes, compositeIndexes: UnifiedSwapStorageCompositeIndexes): Promise<void> {
        await this.initDatabase();
        const {container} = await this.client.database(this.databaseName).containers.createIfNotExists({
            partitionKey: "/id",
            id: this.containerId,
            indexingPolicy: buildIndexingPolicy(indexes, compositeIndexes)
        });
        this.container = container;
    }

    async query(params: Array<Array<QueryParams>>): Promise<Array<UnifiedStoredObject>> {
        let orQuery: string[] = [];
        const values: {name: string, value: any}[] = [];

        let counter = 0;
        for(let orParams of params) {
            if(orParams.length === 0) {
                orQuery = [];
                break;
            }

            let andQuery: string[] = [];
            for(let andParam of orParams) {
                if(Array.isArray(andParam.value)) {
                    if(andParam.value.length === 0) {
                        andQuery = ["0=1"];
                        break;
                    }

                    const tag = "@" + andParam.key + counter.toString(10).padStart(8, "0");
                    andQuery.push("ARRAY_CONTAINS(" + tag + ", c." + andParam.key + ")");
                    values.push({
                        name: tag,
                        value: andParam.value
                    });
                } else {
                    const tag = "@" + andParam.key + counter.toString(10).padStart(8, "0");
                    andQuery.push("c." + andParam.key + " = " + tag);
                    values.push({
                        name: tag,
                        value: andParam.value
                    });
                }
                counter++;
            }
            if(andQuery.length !== 0) orQuery.push("(" + andQuery.join(" AND ") + ")");
        }

        const impossible = orQuery.length > 0 && orQuery.every(value => value === "(0=1)");
        if(impossible) return [];

        orQuery = orQuery.filter(value => value !== "(0=1)");

        const queryToSend = orQuery.length === 0 ? "SELECT * FROM c" : "SELECT * FROM c WHERE " + orQuery.join(" OR ");
        const {resources} = await this.getContainer().items.query<UnifiedStoredObject>({
            query: queryToSend,
            parameters: orQuery.length === 0 ? [] : values
        }).fetchAll();

        resources.forEach(value => {
            const _etag = value._etag;
            delete value._rid;
            delete value._self;
            delete value._etag;
            delete value._attachments;
            delete value._ts;
            value._meta = { initialValue: deepClone(value), _etag }
        });

        return resources;
    }

    async remove(value: UnifiedStoredObject): Promise<void> {
        let etag: string | undefined = value._meta?._etag;
        try {
            if(etag == null) {
                await this.removeItem(value.id);
            } else {
                await this.getContainer().item(value.id, value.id).delete(!this.optimisticConcurrency ? undefined : {
                    accessCondition: {
                        type: "IfMatch",
                        condition: etag
                    }
                });
                etag = undefined;
            }
        } catch (e) {
            const statusCode = parseStatusCode((e as any)?.statusCode ?? (e as any)?.code);
            if(isNotFoundStatusCode(statusCode)) {
                etag = undefined;
                return;
            }
            if(isPreconditionFailedStatusCode(statusCode)) throw new CosmosDBConcurrencyError([value.id], e);
            throw e;
        } finally {
            value._meta = etag == null ? undefined : {_etag: etag};
        }
    }

    async removeAll(value: UnifiedStoredObject[], lenient?: boolean): Promise<void> {
        if(value.length === 0) return;

        const {results, error} = await this.executeBulkOperations(value.map<DeleteOperationInputWithIfMatch>(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id,
            ifMatch: !this.optimisticConcurrency ? undefined : val._meta?._etag
        })), true, lenient);

        value.forEach((val, index) => {
            const statusCode = parseStatusCode(results[index].response?.statusCode ?? results[index].error?.code);
            if(statusCode != null && (statusCode < 300 || isNotFoundStatusCode(statusCode))) val._meta = undefined;
        });

        if(error) throw error;
    }

    async save(value: UnifiedStoredObject): Promise<void> {
        const {_meta, ...toSave} = value;
        let etag: string | undefined = _meta?._etag;
        let batchOperations: PatchOperationInput[] | undefined;

        try {
            if(etag == null) {
                const response = await this.getContainer().items.upsert(toSave);
                etag = response.etag;
            } else {
                const patches = calculatePatch(_meta.initialValue, toSave);
                if(patches.length===0) return;

                if(patches.find(patch => patch.path==="/")!=null) {
                    // Replace whole document
                    if(this.optimisticConcurrency) {
                        const response = await this.getContainer().item(value.id, value.id).replace(toSave, {
                            accessCondition: {
                                type: "IfMatch",
                                condition: etag
                            }
                        });
                        etag = response.etag;
                    } else {
                        const response = await this.getContainer().items.upsert(toSave);
                        etag = response.etag;
                    }
                } else {
                    if(patches.length <= 10) {
                        const response = await this.getContainer().item(value.id, value.id).patch(patches, !this.optimisticConcurrency ? undefined : {
                            accessCondition: {
                                type: "IfMatch",
                                condition: etag
                            }
                        });
                        etag = response.etag;
                    } else {
                        // Need to use batch patch
                        batchOperations = [];
                        for(let i = 0; i < patches.length; i+=10) {
                            batchOperations.push({
                                operationType: "Patch",
                                id: value.id,
                                resourceBody: patches.slice(i, i+10),
                                partitionKey: value.id,
                                ifMatch: batchOperations.length===0 && this.optimisticConcurrency ? etag : undefined
                            });
                        }
                        const results = await this.executeBatchOperations(batchOperations, value.id);
                        // Extract etag from last valid op
                        for(let i = results.length - 1; i >= 0; i--) {
                            const response = results[i];
                            if(response!=null) {
                                etag = response.eTag;
                                break;
                            }
                        }
                    }
                }
            }
        } catch (e) {
            if(batchOperations!=null) throw e;
            const statusCode = parseStatusCode((e as any)?.statusCode ?? (e as any)?.code);
            if(isPreconditionFailedStatusCode(statusCode) || isNotFoundStatusCode(statusCode)) throw new CosmosDBConcurrencyError([value.id], e);
            throw e;
        } finally {
            value._meta = etag == null ? undefined : {_etag: etag, initialValue: _meta?._etag!==etag ? deepClone(toSave) : _meta.initialValue};
        }

    }

    async saveAll(objects: UnifiedStoredObject[], lenient?: boolean): Promise<void> {
        if(objects.length === 0) return;

        const bulkOperations: (ReplaceOperationInput | UpsertOperationInput | PatchOperationInput)[] = [];
        for(let value of objects) {
            const {_meta, ...toSave} = value;
            const etag: string | undefined = _meta?._etag;
            if(etag == null) {
                bulkOperations.push({
                    operationType: "Upsert",
                    resourceBody: toSave,
                    partitionKey: value.id
                });
            } else {
                const patches = calculatePatch(_meta.initialValue, toSave);
                if(patches.length===0) continue;

                if(patches.find(patch => patch.path==="/")!=null) {
                    if(this.optimisticConcurrency) {
                        bulkOperations.push({
                            operationType: "Replace",
                            id: value.id,
                            resourceBody: toSave,
                            partitionKey: value.id,
                            ifMatch: etag
                        });
                    } else {
                        bulkOperations.push({
                            operationType: "Upsert",
                            resourceBody: toSave,
                            partitionKey: value.id
                        });
                    }
                } else {
                    if(patches.length <= 10) {
                        bulkOperations.push({
                            operationType: "Patch",
                            id: value.id,
                            resourceBody: patches,
                            partitionKey: value.id,
                            ifMatch: this.optimisticConcurrency ? etag : undefined
                        });
                    } else {
                        try {
                            await this.save(value);
                        } catch (e) {
                            if(e instanceof CosmosDBConcurrencyError && lenient) continue;
                            throw e;
                        }
                    }
                }
            }
        }

        const {results, error} = await this.executeBulkOperations(bulkOperations, false, lenient);

        const mapping = new Map<string, UnifiedStoredObject>();
        objects.forEach(object => mapping.set(object.id, object));

        results.forEach((value) => {
            const object = mapping.get(value.operationInput.partitionKey as string);
            if(object!=null) {
                const etag = value.response?.eTag;
                if(etag != null) {
                    delete object._meta;
                    object._meta = {_etag: etag, initialValue: deepClone(object) };
                }
            }
        });

        if(error) throw error;
    }

}
