import {
    BulkOperationResult,
    Container,
    CosmosClient,
    OperationInput,
    OperationResponse,
    PartitionKey,
    StatusCodes
} from "@azure/cosmos";
import {CosmosDBConcurrencyError} from "./CosmosDBConcurrencyError";

export function parseStatusCode(statusCodeOrError: any | number | string | undefined): number | undefined {
    if(typeof statusCodeOrError === "object") statusCodeOrError = statusCodeOrError?.statusCode ?? statusCodeOrError?.code;
    if(typeof statusCodeOrError === "number") return statusCodeOrError;
    if(typeof statusCodeOrError === "string") {
        const parsedValue = Number(statusCodeOrError);
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

export function isFailedDependencyStatusCode(statusCode: number | string | undefined): boolean {
    return parseStatusCode(statusCode) === StatusCodes.FailedDependency;
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
    return !(ignoreNotFound && isNotFoundStatusCode(parseStatusCode(result.error)));
}

function getOperationItemId(result: {operationInput: {id?: string, resourceBody?: any}}): string | undefined {
    if("id" in result.operationInput) return result.operationInput.id;
    if("resourceBody" in result.operationInput && result.operationInput.resourceBody != null) {
        const resourceBody = result.operationInput.resourceBody as {id?: string};
        return resourceBody.id;
    }
    return undefined;
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
            if(isNotFoundStatusCode(parseStatusCode(e))) return;
            throw e;
        }
    }

    protected async executeBulkOperations(
        operations: OperationInput[],
        ignoreNotFound: boolean = false,
        lenient: boolean = false
    ): Promise<{
        results: BulkOperationResult[],
        error?: Error
    }> {
        if(operations.length === 0) return {results: []};

        const results = await this.getContainer().items.executeBulkOperations(operations);

        const failedOperations = results.filter(result => didBulkOperationFail(result, ignoreNotFound));
        let error: Error | undefined;
        if(failedOperations.length !== 0) {
            const allConcurrencyFailures = failedOperations.every((result) => {
                const statusCode = parseStatusCode(result.response?.statusCode ?? result.error?.code);
                return isPreconditionFailedStatusCode(statusCode) ||
                    (isNotFoundStatusCode(statusCode) && (result.operationInput.operationType === "Replace" || result.operationInput.operationType === "Patch"));
            });
            if(allConcurrencyFailures) {
                if(!lenient) error = new CosmosDBConcurrencyError(
                    failedOperations
                        .map(result => getOperationItemId(result))
                        .filter((id): id is string => id != null),
                    failedOperations[0].error
                );
            } else {
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

    protected async executeBatchOperations(
        operations: OperationInput[],
        partitionKey: PartitionKey
    ): Promise<OperationResponse[]> {
        if(operations.length === 0) return [];

        const response = await this.getContainer().items.batch(operations, partitionKey);
        const operationResponses = response.result ?? [];
        if(
            response.code != null &&
            isSuccessfulStatusCode(response.code) &&
            operationResponses.length === operations.length
        ) return operationResponses;

        const operationStatusCodes = operationResponses.map(operationResponse => parseStatusCode(operationResponse?.statusCode ?? response.code));
        const failedDependencyIndex = operationStatusCodes.findIndex(isFailedDependencyStatusCode);
        let failedIndex = operationStatusCodes.findIndex(statusCode => {
            if(statusCode==null) return false;
            if(isSuccessfulStatusCode(statusCode)) return false;
            if(isFailedDependencyStatusCode(statusCode)) return false;
            return true;
        });
        if(failedIndex===-1) failedIndex = failedDependencyIndex;

        if(failedIndex !== -1) {
            const failedStatusCode = operationStatusCodes[failedIndex];
            const failedOperation = operations[failedIndex];
            if(isPreconditionFailedStatusCode(failedStatusCode) ||
                (isNotFoundStatusCode(failedStatusCode) && (failedOperation.operationType === "Replace" || failedOperation.operationType === "Patch"))
            ) {
                const id = getOperationItemId({operationInput: failedOperation});
                throw new CosmosDBConcurrencyError(id == null ? [] : [id]);
            }
            throw new Error(
                "Cosmos DB batch operation failed" +
                (failedStatusCode == null ? "" : " with status " + failedStatusCode)
            );
        }

        throw new Error(
            "Cosmos DB batch operation failed" +
            (response.code == null ? "" : " with status " + response.code)
        );
    }

}
