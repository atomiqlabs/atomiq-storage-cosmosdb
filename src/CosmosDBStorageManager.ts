import {DeleteOperationInput, UpsertOperationInput} from "@azure/cosmos";
import {IStorageManager, StorageObject} from "@atomiqlabs/base";
import {CosmosDBBase} from "./CosmosDBBase";

type StoredStorageManagerObject = {
    id: string;
    value: any;
};

export class CosmosDBStorageManager<T extends StorageObject = StorageObject> extends CosmosDBBase implements IStorageManager<T> {

    constructor(name: string, connectionString: string, databaseName: string = "Atomiq") {
        super(name, connectionString, databaseName);
    }

    async init(): Promise<void> {
        await this.initDatabase();
        const {container} = await this.client.database(this.databaseName).containers.createIfNotExists({
            partitionKey: "/id",
            id: this.containerId,
            indexingPolicy: {
                indexingMode: "none"
            }
        });
        this.container = container;
    }

    data: { [p: string]: T } = {};

    async loadData(type: { new(data: any): T }): Promise<T[]> {
        const {resources} = await this.getContainer().items.readAll<StoredStorageManagerObject>().fetchAll();
        this.data = {};
        const allData: T[] = [];
        resources.forEach(({id, value}) => {
            const obj = new type(value);
            this.data[id] = obj;
            allData.push(obj);
        });
        return allData;
    }

    async removeData(hash: string): Promise<void> {
        await this.removeItem(hash);
    }

    async removeDataArr(keys: string[]): Promise<void> {
        await this.executeBulkOperations(keys.map<DeleteOperationInput>(id => ({
            operationType: "Delete",
            id,
            partitionKey: id
        })), true);
    }

    async saveData(hash: string, object: T): Promise<void> {
        await this.getContainer().items.upsert({
            id: hash,
            value: object.serialize()
        });
    }

    async saveDataArr(values: { id: string; object: T }[]): Promise<void> {
        const {error} = await this.executeBulkOperations(values.map<UpsertOperationInput>(val => ({
            operationType: "Upsert",
            resourceBody: {
                id: val.id,
                value: val.object.serialize()
            },
            partitionKey: val.id
        })));
        if(error!=null) throw error;
    }

}
