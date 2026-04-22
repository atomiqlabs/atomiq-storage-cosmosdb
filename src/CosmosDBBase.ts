import {BulkOperationResult, Container, CosmosClient, OperationInput, StatusCodes} from "@azure/cosmos";

export function parseStatusCode(statusCode: number | string | undefined): number | undefined {
    if(typeof statusCode === "number") return statusCode;
    if(typeof statusCode === "string") {
        const parsedValue = Number(statusCode);
        return Number.isFinite(parsedValue) ? parsedValue : undefined;
    }
    return undefined;
}

export function isNotFoundStatusCode(statusCode: number | string | undefined): boolean {
    return parseStatusCode(statusCode) === StatusCodes.NotFound;
}

export function isPreconditionFailedStatusCode(statusCode: number | string | undefined): boolean {
    return parseStatusCode(statusCode) === StatusCodes.PreconditionFailed;
}

export function isSuccessfulStatusCode(statusCode: number): boolean {
    return statusCode >= 200 && statusCode < 300;
}

export function didBulkOperationFail(result: BulkOperationResult, ignoreNotFound: boolean): boolean {
    const statusCode = parseStatusCode(result.response?.statusCode ?? result.error?.code);
    if(statusCode != null) {
        if(isSuccessfulStatusCode(statusCode)) return false;
        if(ignoreNotFound && isNotFoundStatusCode(statusCode)) return false;
    }

    if(result.error == null) return statusCode == null;
    return !(ignoreNotFound && isNotFoundError(result.error));
}

function isNotFoundError(error: any): boolean {
    return isNotFoundStatusCode(error?.statusCode ?? error?.code);
}

export abstract class CosmosDBBase {

    readonly client: CosmosClient;
    readonly databaseName: string;
    protected readonly containerId: string;
    protected container?: Container;

    protected constructor(containerId: string, connectionString: string, databaseName: string = "Atomiq") {
        this.containerId = containerId;
        this.databaseName = databaseName;
        this.client = new CosmosClient(connectionString);
    }

    protected getContainer(): Container {
        if(this.container == null) throw new Error("Database not initialized!");
        return this.container;
    }

    protected async initDatabase(): Promise<void> {
        await this.client.databases.createIfNotExists({
            id: this.databaseName
        });
    }

    protected async removeItem(id: string): Promise<void> {
        try {
            await this.getContainer().item(id, id).delete();
        } catch (e) {
            if(isNotFoundError(e)) return;
            throw e;
        }
    }

    protected async executeBulkOperations(
        operations: OperationInput[],
        ignoreNotFound: boolean = false
    ): Promise<void> {
        if(operations.length === 0) return;

        const results = await this.getContainer().items.executeBulkOperations(operations);
        const failedOperation = results.find(result => didBulkOperationFail(result, ignoreNotFound));
        if(failedOperation == null) return;

        const statusCode = parseStatusCode(failedOperation.response?.statusCode ?? failedOperation.error?.code);
        const message = failedOperation.error?.message;
        throw new Error(
            "Cosmos DB bulk operation failed" +
            (statusCode == null ? "" : " with status " + statusCode) +
            (message == null ? "" : ": " + message)
        );
    }

}
