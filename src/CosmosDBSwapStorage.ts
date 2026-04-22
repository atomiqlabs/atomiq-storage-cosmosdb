import {
    BulkOperationResult,
    CompositePath,
    DeleteOperationInput,
    IndexedPath,
    IndexingPolicy,
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
    didBulkOperationFail,
    isNotFoundStatusCode,
    isPreconditionFailedStatusCode,
    parseStatusCode
} from "./CosmosDBBase";
import {CosmosDBConcurrencyError} from "./CosmosDBConcurrencyError";

type DeleteOperationInputWithIfMatch = DeleteOperationInput & {
    ifMatch?: string;
};

function getBulkOperationItemId(result: BulkOperationResult): string | undefined {
    if("id" in result.operationInput) return result.operationInput.id;
    if("resourceBody" in result.operationInput && result.operationInput.resourceBody != null) {
        const resourceBody = result.operationInput.resourceBody as {id?: string};
        return resourceBody.id;
    }
    return undefined;
}

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

export class CosmosDBSwapStorage extends CosmosDBBase implements IUnifiedStorage<UnifiedSwapStorageIndexes, UnifiedSwapStorageCompositeIndexes> {

    constructor(chainId: string, connectionString: string, databaseName: string = "Atomiq") {
        super(chainId, connectionString, databaseName);
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

        resources.forEach(value => value._meta = {_etag: value._etag});

        return resources;
    }

    async remove(value: UnifiedStoredObject): Promise<void> {
        let etag: string | undefined = value._meta?._etag;
        try {
            if(etag == null) {
                await this.removeItem(value.id);
            } else {
                await this.getContainer().item(value.id, value.id).delete({
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

        const results = await this.getContainer().items.executeBulkOperations(value.map<DeleteOperationInputWithIfMatch>(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id,
            ifMatch: val._meta?._etag
        })));

        try {
            const failedOperations = results.filter(result => didBulkOperationFail(result, true));
            if(failedOperations.length !== 0) {
                const allConcurrencyFailures = failedOperations.every(result =>
                    isPreconditionFailedStatusCode(result.response?.statusCode ?? result.error?.code)
                );
                if(allConcurrencyFailures) {
                    if(!lenient) throw new CosmosDBConcurrencyError(
                        failedOperations
                            .map(result => getBulkOperationItemId(result))
                            .filter((id): id is string => id != null),
                        failedOperations[0].error
                    );
                } else {
                    const failedOperation = failedOperations[0];
                    const statusCode = parseStatusCode(failedOperation.response?.statusCode ?? failedOperation.error?.code);
                    const message = failedOperation.error?.message;
                    throw new Error(
                        "Cosmos DB bulk operation failed" +
                        (statusCode == null ? "" : " with status " + statusCode) +
                        (message == null ? "" : ": " + message)
                    );
                }
            }
        } finally {
            value.forEach((val, index) => {
                const statusCode = parseStatusCode(results[index].response?.statusCode ?? results[index].error?.code);
                if(statusCode != null && (statusCode < 300 || isNotFoundStatusCode(statusCode))) val._meta = undefined;
            });
        }
    }

    async save(value: UnifiedStoredObject): Promise<void> {
        const {_meta, ...toSave} = value;
        let etag: string | undefined = _meta?._etag;
        try {
            const response = etag == null ?
                await this.getContainer().items.upsert(toSave) :
                await this.getContainer().item(value.id, value.id).replace(toSave, {
                    accessCondition: {
                        type: "IfMatch",
                        condition: etag
                    }
                });
            etag = response.etag;
        } catch (e) {
            const statusCode = parseStatusCode((e as any)?.statusCode ?? (e as any)?.code);
            if(isPreconditionFailedStatusCode(statusCode) || isNotFoundStatusCode(statusCode)) throw new CosmosDBConcurrencyError([value.id], e);
            throw e;
        } finally {
            value._meta = etag == null ? undefined : {_etag: etag};
        }
    }

    async saveAll(value: UnifiedStoredObject[], lenient?: boolean): Promise<void> {
        if(value.length === 0) return;

        const results = await this.getContainer().items.executeBulkOperations(value.map<ReplaceOperationInput | UpsertOperationInput>(val => {
            const {_meta, ...toSave} = val;
            const etag: string | undefined = _meta?._etag;
            if(etag == null) {
                return {
                    operationType: "Upsert",
                    resourceBody: toSave,
                    partitionKey: val.id
                };
            }
            return {
                operationType: "Replace",
                id: val.id,
                resourceBody: toSave,
                partitionKey: val.id,
                ifMatch: etag
            };
        }));

        try {
            const failedOperations = results.filter(result => didBulkOperationFail(result, false));
            if(failedOperations.length !== 0) {
                const allConcurrencyFailures = failedOperations.every(result => {
                    const statusCode = parseStatusCode(result.response?.statusCode ?? result.error?.code);
                    return isPreconditionFailedStatusCode(statusCode) ||
                        (isNotFoundStatusCode(statusCode) && result.operationInput.operationType === "Replace");
                });
                if(allConcurrencyFailures) {
                    if(!lenient) throw new CosmosDBConcurrencyError(
                        failedOperations
                            .map(result => getBulkOperationItemId(result))
                            .filter((id): id is string => id != null),
                        failedOperations[0].error
                    );
                } else {
                    const failedOperation = failedOperations[0];
                    const statusCode = parseStatusCode(failedOperation.response?.statusCode ?? failedOperation.error?.code);
                    const message = failedOperation.error?.message;
                    throw new Error(
                        "Cosmos DB bulk operation failed" +
                        (statusCode == null ? "" : " with status " + statusCode) +
                        (message == null ? "" : ": " + message)
                    );
                }
            }
        } finally {
            value.forEach((val, index) => {
                const etag = results[index].response?.eTag;
                if(etag != null) val._meta = {_etag: etag};
            });
        }

    }

}
