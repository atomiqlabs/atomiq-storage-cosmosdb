import {
    CompositePath,
    DeleteOperationInput,
    IndexedPath,
    IndexingPolicy,
    UpsertOperationInput
} from "@azure/cosmos";
import {
    IUnifiedStorage,
    QueryParams,
    UnifiedStoredObject,
    UnifiedSwapStorageCompositeIndexes,
    UnifiedSwapStorageIndexes
} from "@atomiqlabs/sdk";
import {CosmosDBBase} from "./CosmosDBBase";

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

        return resources;
    }

    async remove(value: UnifiedStoredObject): Promise<void> {
        await this.removeItem(value.id);
    }

    async removeAll(value: UnifiedStoredObject[]): Promise<void> {
        await this.executeBulkOperations(value.map<DeleteOperationInput>(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id
        })), true);
    }

    async save(value: UnifiedStoredObject): Promise<void> {
        await this.getContainer().items.upsert(value);
    }

    async saveAll(value: UnifiedStoredObject[]): Promise<void> {
        await this.executeBulkOperations(value.map<UpsertOperationInput>(val => ({
            operationType: "Upsert",
            resourceBody: val,
            partitionKey: val.id
        })));
    }

}
