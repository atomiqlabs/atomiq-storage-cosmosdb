import { IUnifiedStorage, QueryParams, UnifiedStoredObject, UnifiedSwapStorageCompositeIndexes, UnifiedSwapStorageIndexes } from "@atomiqlabs/sdk";
import { CosmosDBBase } from "./CosmosDBBase";
export declare class CosmosDBSwapPatchStorage extends CosmosDBBase implements IUnifiedStorage<UnifiedSwapStorageIndexes, UnifiedSwapStorageCompositeIndexes> {
    optimisticConcurrency: boolean;
    constructor(chainId: string, connectionString: string, databaseName?: string, optimisticConcurrency?: boolean);
    init(indexes: UnifiedSwapStorageIndexes, compositeIndexes: UnifiedSwapStorageCompositeIndexes): Promise<void>;
    query(params: Array<Array<QueryParams>>): Promise<Array<UnifiedStoredObject>>;
    remove(value: UnifiedStoredObject): Promise<void>;
    removeAll(value: UnifiedStoredObject[], lenient?: boolean): Promise<void>;
    save(value: UnifiedStoredObject): Promise<void>;
    saveAll(objects: UnifiedStoredObject[], lenient?: boolean): Promise<void>;
}
