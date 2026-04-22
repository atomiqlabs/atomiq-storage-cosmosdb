"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBConcurrencyError = void 0;
class CosmosDBConcurrencyError extends Error {
    constructor(itemIds, cause) {
        super(itemIds.length === 1 ?
            "Cosmos DB optimistic concurrency check failed for item: " + itemIds[0] :
            "Cosmos DB optimistic concurrency check failed for items: " + itemIds.join(", "));
        this.name = "CosmosDBConcurrencyError";
        this.itemIds = itemIds;
        this.cause = cause;
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.CosmosDBConcurrencyError = CosmosDBConcurrencyError;
