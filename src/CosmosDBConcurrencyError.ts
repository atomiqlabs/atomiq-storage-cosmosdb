
export class CosmosDBConcurrencyError extends Error {

    readonly itemIds: string[];
    readonly cause: unknown;

    constructor(itemIds: string[], cause?: unknown) {
        super(
            itemIds.length === 1 ?
                "Cosmos DB optimistic concurrency check failed for item: " + itemIds[0] :
                "Cosmos DB optimistic concurrency check failed for items: " + itemIds.join(", ")
        );
        this.name = "CosmosDBConcurrencyError";
        this.itemIds = itemIds;
        this.cause = cause;
        Object.setPrototypeOf(this, new.target.prototype);
    }

}
