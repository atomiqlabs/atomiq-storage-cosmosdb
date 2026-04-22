"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBSwapStorage = void 0;
const CosmosDBBase_1 = require("./CosmosDBBase");
const CosmosDBConcurrencyError_1 = require("./CosmosDBConcurrencyError");
function getBulkOperationItemId(result) {
    if ("id" in result.operationInput)
        return result.operationInput.id;
    if ("resourceBody" in result.operationInput && result.operationInput.resourceBody != null) {
        const resourceBody = result.operationInput.resourceBody;
        return resourceBody.id;
    }
    return undefined;
}
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
        if (index.key === "id")
            return;
        includedPathsMap.set(toScalarPath(index.key), {
            path: toScalarPath(index.key)
        });
    });
    compositeIndexes.forEach(index => {
        index.keys.forEach(key => {
            if (key === "id")
                return;
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
        resources.forEach(value => value._meta = { _etag: value._etag });
        return resources;
    }
    async remove(value) {
        let etag = value._meta?._etag;
        try {
            if (etag == null) {
                await this.removeItem(value.id);
            }
            else {
                await this.getContainer().item(value.id, value.id).delete({
                    accessCondition: {
                        type: "IfMatch",
                        condition: etag
                    }
                });
                etag = undefined;
            }
        }
        catch (e) {
            const statusCode = (0, CosmosDBBase_1.parseStatusCode)(e?.statusCode ?? e?.code);
            if ((0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode)) {
                etag = undefined;
                return;
            }
            if ((0, CosmosDBBase_1.isPreconditionFailedStatusCode)(statusCode))
                throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError([value.id], e);
            throw e;
        }
        finally {
            value._meta = etag == null ? undefined : { _etag: etag };
        }
    }
    async removeAll(value, lenient) {
        if (value.length === 0)
            return;
        const results = await this.getContainer().items.executeBulkOperations(value.map(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id,
            ifMatch: val._meta?._etag
        })));
        try {
            const failedOperations = results.filter(result => (0, CosmosDBBase_1.didBulkOperationFail)(result, true));
            if (failedOperations.length !== 0) {
                const allConcurrencyFailures = failedOperations.every(result => (0, CosmosDBBase_1.isPreconditionFailedStatusCode)(result.response?.statusCode ?? result.error?.code));
                if (allConcurrencyFailures) {
                    if (!lenient)
                        throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError(failedOperations
                            .map(result => getBulkOperationItemId(result))
                            .filter((id) => id != null), failedOperations[0].error);
                }
                else {
                    const failedOperation = failedOperations[0];
                    const statusCode = (0, CosmosDBBase_1.parseStatusCode)(failedOperation.response?.statusCode ?? failedOperation.error?.code);
                    const message = failedOperation.error?.message;
                    throw new Error("Cosmos DB bulk operation failed" +
                        (statusCode == null ? "" : " with status " + statusCode) +
                        (message == null ? "" : ": " + message));
                }
            }
        }
        finally {
            value.forEach((val, index) => {
                const statusCode = (0, CosmosDBBase_1.parseStatusCode)(results[index].response?.statusCode ?? results[index].error?.code);
                if (statusCode != null && (statusCode < 300 || (0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode)))
                    val._meta = undefined;
            });
        }
    }
    async save(value) {
        const { _meta, ...toSave } = value;
        let etag = _meta?._etag;
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
        }
        catch (e) {
            const statusCode = (0, CosmosDBBase_1.parseStatusCode)(e?.statusCode ?? e?.code);
            if ((0, CosmosDBBase_1.isPreconditionFailedStatusCode)(statusCode) || (0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode))
                throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError([value.id], e);
            throw e;
        }
        finally {
            value._meta = etag == null ? undefined : { _etag: etag };
        }
    }
    async saveAll(value, lenient) {
        if (value.length === 0)
            return;
        const results = await this.getContainer().items.executeBulkOperations(value.map(val => {
            const { _meta, ...toSave } = val;
            const etag = _meta?._etag;
            if (etag == null) {
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
            const failedOperations = results.filter(result => (0, CosmosDBBase_1.didBulkOperationFail)(result, false));
            if (failedOperations.length !== 0) {
                const allConcurrencyFailures = failedOperations.every(result => {
                    const statusCode = (0, CosmosDBBase_1.parseStatusCode)(result.response?.statusCode ?? result.error?.code);
                    return (0, CosmosDBBase_1.isPreconditionFailedStatusCode)(statusCode) ||
                        ((0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode) && result.operationInput.operationType === "Replace");
                });
                if (allConcurrencyFailures) {
                    if (!lenient)
                        throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError(failedOperations
                            .map(result => getBulkOperationItemId(result))
                            .filter((id) => id != null), failedOperations[0].error);
                }
                else {
                    const failedOperation = failedOperations[0];
                    const statusCode = (0, CosmosDBBase_1.parseStatusCode)(failedOperation.response?.statusCode ?? failedOperation.error?.code);
                    const message = failedOperation.error?.message;
                    throw new Error("Cosmos DB bulk operation failed" +
                        (statusCode == null ? "" : " with status " + statusCode) +
                        (message == null ? "" : ": " + message));
                }
            }
        }
        finally {
            value.forEach((val, index) => {
                const etag = results[index].response?.eTag;
                if (etag != null)
                    val._meta = { _etag: etag };
            });
        }
    }
}
exports.CosmosDBSwapStorage = CosmosDBSwapStorage;
