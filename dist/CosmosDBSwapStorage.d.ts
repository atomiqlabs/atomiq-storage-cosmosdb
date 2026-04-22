import { IUnifiedStorage, QueryParams, UnifiedStoredObject, UnifiedSwapStorageCompositeIndexes, UnifiedSwapStorageIndexes } from "@atomiqlabs/sdk";
import { CosmosDBBase } from "./CosmosDBBase";
export declare class CosmosDBSwapStorage extends CosmosDBBase implements IUnifiedStorage<UnifiedSwapStorageIndexes, UnifiedSwapStorageCompositeIndexes> {
    constructor(chainId: string, connectionString: string, databaseName?: string);
    init(indexes: UnifiedSwapStorageIndexes, compositeIndexes: UnifiedSwapStorageCompositeIndexes): Promise<void>;
    query(params: Array<Array<QueryParams>>): Promise<Array<UnifiedStoredObject>>;
    remove(value: UnifiedStoredObject): Promise<void>;
    removeAll(value: UnifiedStoredObject[]): Promise<void>;
    save(value: UnifiedStoredObject): Promise<void>;
    saveAll(value: UnifiedStoredObject[]): Promise<void>;
}
