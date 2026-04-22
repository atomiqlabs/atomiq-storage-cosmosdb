import { BulkOperationResult, Container, CosmosClient, OperationInput } from "@azure/cosmos";
export declare function parseStatusCode(statusCode: number | string | undefined): number | undefined;
export declare function isNotFoundStatusCode(statusCode: number | string | undefined): boolean;
export declare function isPreconditionFailedStatusCode(statusCode: number | string | undefined): boolean;
export declare function isSuccessfulStatusCode(statusCode: number): boolean;
export declare function didBulkOperationFail(result: BulkOperationResult, ignoreNotFound: boolean): boolean;
export declare abstract class CosmosDBBase {
    readonly client: CosmosClient;
    readonly databaseName: string;
    protected readonly containerId: string;
    protected container?: Container;
    protected constructor(containerId: string, connectionString: string, databaseName?: string);
    protected getContainer(): Container;
    protected initDatabase(): Promise<void>;
    protected removeItem(id: string): Promise<void>;
    protected executeBulkOperations(operations: OperationInput[], ignoreNotFound?: boolean): Promise<void>;
}
