"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBSwapPatchStorage = void 0;
const CosmosDBBase_1 = require("./CosmosDBBase");
const CosmosDBConcurrencyError_1 = require("./CosmosDBConcurrencyError");
const Utils_1 = require("./Utils");
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
class CosmosDBSwapPatchStorage extends CosmosDBBase_1.CosmosDBBase {
    constructor(chainId, connectionString, databaseName = "Atomiq", optimisticConcurrency = false) {
        super(chainId, connectionString, databaseName);
        this.optimisticConcurrency = optimisticConcurrency;
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
        resources.forEach(value => {
            const _etag = value._etag;
            delete value._rid;
            delete value._self;
            delete value._etag;
            delete value._attachments;
            delete value._ts;
            value._meta = { initialValue: (0, Utils_1.deepClone)(value), _etag };
        });
        return resources;
    }
    async remove(value) {
        let etag = value._meta?._etag;
        try {
            if (etag == null) {
                await this.removeItem(value.id);
            }
            else {
                await this.getContainer().item(value.id, value.id).delete(!this.optimisticConcurrency ? undefined : {
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
        const { results, error } = await this.executeBulkOperations(value.map(val => ({
            operationType: "Delete",
            id: val.id,
            partitionKey: val.id,
            ifMatch: !this.optimisticConcurrency ? undefined : val._meta?._etag
        })), true, lenient);
        value.forEach((val, index) => {
            const statusCode = (0, CosmosDBBase_1.parseStatusCode)(results[index].response?.statusCode ?? results[index].error?.code);
            if (statusCode != null && (statusCode < 300 || (0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode)))
                val._meta = undefined;
        });
        if (error)
            throw error;
    }
    async save(value) {
        const { _meta, ...toSave } = value;
        let etag = _meta?._etag;
        let batchOperations;
        try {
            if (etag == null) {
                const response = await this.getContainer().items.upsert(toSave);
                etag = response.etag;
            }
            else {
                const patches = (0, Utils_1.calculatePatch)(_meta.initialValue, toSave);
                if (patches.length === 0)
                    return;
                if (patches.find(patch => patch.path === "/") != null) {
                    // Replace whole document
                    if (this.optimisticConcurrency) {
                        const response = await this.getContainer().item(value.id, value.id).replace(toSave, {
                            accessCondition: {
                                type: "IfMatch",
                                condition: etag
                            }
                        });
                        etag = response.etag;
                    }
                    else {
                        const response = await this.getContainer().items.upsert(toSave);
                        etag = response.etag;
                    }
                }
                else {
                    if (patches.length <= 10) {
                        const response = await this.getContainer().item(value.id, value.id).patch(patches, !this.optimisticConcurrency ? undefined : {
                            accessCondition: {
                                type: "IfMatch",
                                condition: etag
                            }
                        });
                        etag = response.etag;
                    }
                    else {
                        // Need to use batch patch
                        batchOperations = [];
                        for (let i = 0; i < patches.length; i += 10) {
                            batchOperations.push({
                                operationType: "Patch",
                                id: value.id,
                                resourceBody: { operations: patches.slice(i, i + 10) },
                                partitionKey: value.id,
                                ifMatch: batchOperations.length === 0 && this.optimisticConcurrency ? etag : undefined
                            });
                        }
                        const results = await this.executeBatchOperations(batchOperations, value.id);
                        // Extract etag from last valid op
                        for (let i = results.length - 1; i >= 0; i--) {
                            const response = results[i];
                            if (response != null) {
                                etag = response.eTag;
                                break;
                            }
                        }
                    }
                }
            }
        }
        catch (e) {
            if (batchOperations != null)
                throw e;
            const statusCode = (0, CosmosDBBase_1.parseStatusCode)(e?.statusCode ?? e?.code);
            if ((0, CosmosDBBase_1.isPreconditionFailedStatusCode)(statusCode) || (0, CosmosDBBase_1.isNotFoundStatusCode)(statusCode))
                throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError([value.id], e);
            throw e;
        }
        finally {
            value._meta = etag == null ? undefined : { _etag: etag, initialValue: _meta?._etag !== etag ? (0, Utils_1.deepClone)(toSave) : _meta.initialValue };
        }
    }
    async saveAll(objects, lenient) {
        if (objects.length === 0)
            return;
        const bulkOperations = [];
        for (let value of objects) {
            const { _meta, ...toSave } = value;
            const etag = _meta?._etag;
            if (etag == null) {
                bulkOperations.push({
                    operationType: "Upsert",
                    resourceBody: toSave,
                    partitionKey: value.id
                });
            }
            else {
                const patches = (0, Utils_1.calculatePatch)(_meta.initialValue, toSave);
                if (patches.length === 0)
                    continue;
                if (patches.find(patch => patch.path === "/") != null) {
                    if (this.optimisticConcurrency) {
                        bulkOperations.push({
                            operationType: "Replace",
                            id: value.id,
                            resourceBody: toSave,
                            partitionKey: value.id,
                            ifMatch: etag
                        });
                    }
                    else {
                        bulkOperations.push({
                            operationType: "Upsert",
                            resourceBody: toSave,
                            partitionKey: value.id
                        });
                    }
                }
                else {
                    if (patches.length <= 10) {
                        bulkOperations.push({
                            operationType: "Patch",
                            id: value.id,
                            resourceBody: { operations: patches },
                            partitionKey: value.id,
                            ifMatch: this.optimisticConcurrency ? etag : undefined
                        });
                    }
                    else {
                        try {
                            await this.save(value);
                        }
                        catch (e) {
                            if (e instanceof CosmosDBConcurrencyError_1.CosmosDBConcurrencyError && lenient)
                                continue;
                            throw e;
                        }
                    }
                }
            }
        }
        const { results, error } = await this.executeBulkOperations(bulkOperations, false, lenient);
        const mapping = new Map();
        objects.forEach(object => mapping.set(object.id, object));
        results.forEach((value) => {
            const object = mapping.get(value.operationInput.partitionKey);
            if (object != null) {
                const etag = value.response?.eTag;
                if (etag != null) {
                    delete object._meta;
                    object._meta = { _etag: etag, initialValue: (0, Utils_1.deepClone)(object) };
                }
            }
        });
        if (error)
            throw error;
    }
}
exports.CosmosDBSwapPatchStorage = CosmosDBSwapPatchStorage;
