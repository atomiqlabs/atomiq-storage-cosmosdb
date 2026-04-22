export declare class CosmosDBConcurrencyError extends Error {
    readonly itemIds: string[];
    readonly cause: unknown;
    constructor(itemIds: string[], cause?: unknown);
}
