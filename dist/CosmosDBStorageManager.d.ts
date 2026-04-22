import { IStorageManager, StorageObject } from "@atomiqlabs/base";
import { CosmosDBBase } from "./CosmosDBBase";
export declare class CosmosDBStorageManager<T extends StorageObject = StorageObject> extends CosmosDBBase implements IStorageManager<T> {
    constructor(name: string, connectionString: string, databaseName?: string);
    init(): Promise<void>;
    data: {
        [p: string]: T;
    };
    loadData(type: {
        new (data: any): T;
    }): Promise<T[]>;
    removeData(hash: string): Promise<void>;
    removeDataArr(keys: string[]): Promise<void>;
    saveData(hash: string, object: T): Promise<void>;
    saveDataArr(values: {
        id: string;
        object: T;
    }[]): Promise<void>;
}
