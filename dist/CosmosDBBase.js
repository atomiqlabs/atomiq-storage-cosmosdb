"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBBase = exports.didBulkOperationFail = exports.isSuccessfulStatusCode = exports.isFailedDependencyStatusCode = exports.isPreconditionFailedStatusCode = exports.isNotFoundStatusCode = exports.parseStatusCode = void 0;
const cosmos_1 = require("@azure/cosmos");
const CosmosDBConcurrencyError_1 = require("./CosmosDBConcurrencyError");
function parseStatusCode(statusCodeOrError) {
    if (typeof statusCodeOrError === "object")
        statusCodeOrError = statusCodeOrError?.statusCode ?? statusCodeOrError?.code;
    if (typeof statusCodeOrError === "number")
        return statusCodeOrError;
    if (typeof statusCodeOrError === "string") {
        const parsedValue = Number(statusCodeOrError);
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
function isFailedDependencyStatusCode(statusCode) {
    return parseStatusCode(statusCode) === cosmos_1.StatusCodes.FailedDependency;
}
exports.isFailedDependencyStatusCode = isFailedDependencyStatusCode;
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
    return !(ignoreNotFound && isNotFoundStatusCode(parseStatusCode(result.error)));
}
exports.didBulkOperationFail = didBulkOperationFail;
function getOperationItemId(result) {
    if ("id" in result.operationInput)
        return result.operationInput.id;
    if ("resourceBody" in result.operationInput && result.operationInput.resourceBody != null) {
        const resourceBody = result.operationInput.resourceBody;
        return resourceBody.id;
    }
    return undefined;
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
            if (isNotFoundStatusCode(parseStatusCode(e)))
                return;
            throw e;
        }
    }
    async executeBulkOperations(operations, ignoreNotFound = false, lenient = false) {
        if (operations.length === 0)
            return { results: [] };
        const results = await this.getContainer().items.executeBulkOperations(operations);
        const failedOperations = results.filter(result => didBulkOperationFail(result, ignoreNotFound));
        let error;
        if (failedOperations.length !== 0) {
            const allConcurrencyFailures = failedOperations.every((result) => {
                const statusCode = parseStatusCode(result.response?.statusCode ?? result.error?.code);
                return isPreconditionFailedStatusCode(statusCode) ||
                    (isNotFoundStatusCode(statusCode) && (result.operationInput.operationType === "Replace" || result.operationInput.operationType === "Patch"));
            });
            if (allConcurrencyFailures) {
                if (!lenient)
                    error = new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError(failedOperations
                        .map(result => getOperationItemId(result))
                        .filter((id) => id != null), failedOperations[0].error);
            }
            else {
                const statusCode = parseStatusCode(failedOperations[0].response?.statusCode ?? failedOperations[0].error?.code);
                const message = failedOperations[0].error?.message;
                error = new Error("Cosmos DB bulk operation failed" +
                    (statusCode == null ? "" : " with status " + statusCode) +
                    (message == null ? "" : ": " + message));
            }
        }
        return {
            results,
            error
        };
    }
    async executeBatchOperations(operations, partitionKey) {
        if (operations.length === 0)
            return [];
        const response = await this.getContainer().items.batch(operations, partitionKey);
        const operationResponses = response.result ?? [];
        if (response.code != null &&
            isSuccessfulStatusCode(response.code) &&
            operationResponses.length === operations.length)
            return operationResponses;
        const operationStatusCodes = operationResponses.map(operationResponse => parseStatusCode(operationResponse?.statusCode ?? response.code));
        const failedDependencyIndex = operationStatusCodes.findIndex(isFailedDependencyStatusCode);
        let failedIndex = operationStatusCodes.findIndex(statusCode => {
            if (statusCode == null)
                return false;
            if (isSuccessfulStatusCode(statusCode))
                return false;
            if (isFailedDependencyStatusCode(statusCode))
                return false;
            return true;
        });
        if (failedIndex === -1)
            failedIndex = failedDependencyIndex;
        if (failedIndex !== -1) {
            const failedStatusCode = operationStatusCodes[failedIndex];
            const failedOperation = operations[failedIndex];
            if (isPreconditionFailedStatusCode(failedStatusCode) ||
                (isNotFoundStatusCode(failedStatusCode) && (failedOperation.operationType === "Replace" || failedOperation.operationType === "Patch"))) {
                const id = getOperationItemId({ operationInput: failedOperation });
                throw new CosmosDBConcurrencyError_1.CosmosDBConcurrencyError(id == null ? [] : [id]);
            }
            throw new Error("Cosmos DB batch operation failed" +
                (failedStatusCode == null ? "" : " with status " + failedStatusCode));
        }
        throw new Error("Cosmos DB batch operation failed" +
            (response.code == null ? "" : " with status " + response.code));
    }
}
exports.CosmosDBBase = CosmosDBBase;
