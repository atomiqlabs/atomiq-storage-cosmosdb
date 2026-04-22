"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBSwapStorage = void 0;
const CosmosDBBase_1 = require("./CosmosDBBase");
function toCosmosPathSegment(value) {
    return /^[A-Za-z0-9_]+$/.test(value) ? value : JSON.stringify(value);
}
function toScalarPath(value) {
    return "/" + toCosmosPathSegment(value) + "/?";
}
function toCompositeScalarPath(value) {
    return "/" + toCosmosPathSegment(value);
}
function buildIndexingPolicy(indexes, compositeIndexes) {
    const includedPathsMap = new Map();
    indexes.forEach(index => {
        includedPathsMap.set(toScalarPath(index.key), {
            path: toScalarPath(index.key)
        });
    });
    compositeIndexes.forEach(index => {
        index.keys.forEach(key => {
            const path = toScalarPath(key);
            if (includedPathsMap.has(path))
                return;
            includedPathsMap.set(path, { path });
        });
    });
    const cosmosCompositeIndexes = compositeIndexes
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
            { path: "/*" }
        ],
        compositeIndexes: cosmosCompositeIndexes
    };
}
class CosmosDBSwapStorage extends CosmosDBBase_1.CosmosDBBase {
    constructor(chainId, connectionString, databaseName = "Atomiq") {
        super(chainId, connectionString, databaseName);
    }
    async init(indexes, compositeIndexes) {
        await this.initDatabase();
        const { container } = await this.client.database(this.databaseName).containers.createIfNotExists({
            partitionKey: "/id",
            id: this.containerId,
            indexingPolicy: buildIndexingPolicy(indexes, compositeIndexes)
        });
        this.container = container;
    }
    async query(params) {
        let orQuery = [];
        const values = [];
        let counter = 0;
        for (let orParams of params) {
            if (orParams.length === 0) {
                orQuery = [];
                break;
            }
            let andQuery = [];
            for (let andParam of orParams) {
                if (Array.isArray(andParam.value)) {
                    if (andParam.value.length === 0) {
                        andQuery = ["0=1"];
                        break;
                    }
                    const tag = "@" + andParam.key + counter.toString(10).padStart(8, "0");
                    andQuery.push("ARRAY_CONTAINS(" + tag + ", c." + andParam.key + ")");
                    values.push({
                        name: tag,
                        value: andParam.value
                    });
                }
                else {
                    const tag = "@" + andParam.key + counter.toString(10).padStart(8, "0");
                    andQuery.push("c." + andParam.key + " = " + tag);
                    values.push({
                        name: tag,
                        value: andParam.value
                    });
                }
                counter++;
            }
            if (andQuery.length !== 0)
                orQuery.push("(" + andQuery.join(" AND ") + ")");
        }
        const impossible = orQuery.length > 0 && orQuery.every(value => value === "(0=1)");
        if (impossible)
            return [];
        orQuery = orQuery.filter(value => value !== "(0=1)");
        const queryToSend = orQuery.length === 0 ? "SELECT * FROM c" : "SELECT * FROM c WHERE " + orQuery.join(" OR ");
        const { resources } = await this.getContainer().items.query({
            query: queryToSend,
            parameters: orQuery.length === 0 ? [] : values
        }).fetchAll();
        return resources;
    }
    async remove(value) {
        await this.removeItem(value.id);
    }
    async removeAll(value) {
        await this.executeBulkOperations(value.map(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id
        })), true);
    }
    async save(value) {
        await this.getContainer().items.upsert(value);
    }
    async saveAll(value) {
        await this.executeBulkOperations(value.map(val => ({
            operationType: "Upsert",
            resourceBody: val,
            partitionKey: val.id
        })));
    }
}
exports.CosmosDBSwapStorage = CosmosDBSwapStorage;
