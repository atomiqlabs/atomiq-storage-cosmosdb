import { Container, CosmosClient, OperationInput } from "@azure/cosmos";
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
