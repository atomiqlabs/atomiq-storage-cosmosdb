"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBBase = exports.didBulkOperationFail = exports.isSuccessfulStatusCode = exports.isPreconditionFailedStatusCode = exports.isNotFoundStatusCode = exports.parseStatusCode = void 0;
const cosmos_1 = require("@azure/cosmos");
function parseStatusCode(statusCode) {
    if (typeof statusCode === "number")
        return statusCode;
    if (typeof statusCode === "string") {
        const parsedValue = Number(statusCode);
        return Number.isFinite(parsedValue) ? parsedValue : undefined;
    }
    return undefined;
}
exports.parseStatusCode = parseStatusCode;
function isNotFoundStatusCode(statusCode) {
    return parseStatusCode(statusCode) === cosmos_1.StatusCodes.NotFound;
}
exports.isNotFoundStatusCode = isNotFoundStatusCode;
function isPreconditionFailedStatusCode(statusCode) {
    return parseStatusCode(statusCode) === cosmos_1.StatusCodes.PreconditionFailed;
}
exports.isPreconditionFailedStatusCode = isPreconditionFailedStatusCode;
function isSuccessfulStatusCode(statusCode) {
    return statusCode >= 200 && statusCode < 300;
}
exports.isSuccessfulStatusCode = isSuccessfulStatusCode;
function didBulkOperationFail(result, ignoreNotFound) {
    const statusCode = parseStatusCode(result.response?.statusCode ?? result.error?.code);
    if (statusCode != null) {
        if (isSuccessfulStatusCode(statusCode))
            return false;
        if (ignoreNotFound && isNotFoundStatusCode(statusCode))
            return false;
    }
    if (result.error == null)
        return statusCode == null;
    return !(ignoreNotFound && isNotFoundError(result.error));
}
exports.didBulkOperationFail = didBulkOperationFail;
function isNotFoundError(error) {
    return isNotFoundStatusCode(error?.statusCode ?? error?.code);
}
class CosmosDBBase {
    constructor(containerId, connectionString, databaseName = "Atomiq") {
        this.containerId = containerId;
        this.databaseName = databaseName;
        this.client = new cosmos_1.CosmosClient(connectionString);
    }
    getContainer() {
        if (this.container == null)
            throw new Error("Database not initialized!");
        return this.container;
    }
    async initDatabase() {
        await this.client.databases.createIfNotExists({
            id: this.databaseName
        });
    }
    async removeItem(id) {
        try {
            await this.getContainer().item(id, id).delete();
        }
        catch (e) {
            if (isNotFoundError(e))
                return;
            throw e;
        }
    }
    async executeBulkOperations(operations, ignoreNotFound = false) {
        if (operations.length === 0)
            return;
        const results = await this.getContainer().items.executeBulkOperations(operations);
        const failedOperation = results.find(result => didBulkOperationFail(result, ignoreNotFound));
        if (failedOperation == null)
            return;
        const statusCode = parseStatusCode(failedOperation.response?.statusCode ?? failedOperation.error?.code);
        const message = failedOperation.error?.message;
        throw new Error("Cosmos DB bulk operation failed" +
            (statusCode == null ? "" : " with status " + statusCode) +
            (message == null ? "" : ": " + message));
    }
}
exports.CosmosDBBase = CosmosDBBase;
