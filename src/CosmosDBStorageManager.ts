import {Container, CosmosClient, DeleteOperationInput, UpsertOperationInput} from "@azure/cosmos";
import {IStorageManager, QueryParams, StorageObject, UnifiedStoredObject} from "@atomiqlabs/sdk";

export class CosmosDBStorageManager<T extends StorageObject> implements IStorageManager<T> {

    client: CosmosClient;
    name: string;
    container: Container;

    constructor(name: string) {
        this.name = name;
        this.client = new CosmosClient(process.env.COSMOS_CONECTION_STRING);
    }

    async init(): Promise<void> {
        const {container} = await this.client.database("Atomiq").containers.createIfNotExists({
            partitionKey: "/id",
            id: this.name
        });
        this.container = container;
    }

    data: { [p: string]: T };

    async loadData(type: { new(data: any): T }): Promise<T[]> {
        const {resources} = await this.container.items.readAll().fetchAll();
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
        await this.container.item(hash, hash).delete();
    }

    async removeDataArr(keys: string[]): Promise<void> {
        let temp: DeleteOperationInput[] = [];
        for(let id of keys) {
            temp.push({
                operationType: "Delete",
                id: id,
                partitionKey: id
            });
            if(temp.length>=100) {
                await this.container.items.bulk(temp, {continueOnError: true});
                temp = [];
            }
        }
        if(temp.length>0) await this.container.items.bulk(temp);
    }

    async saveData(hash: string, object: T): Promise<void> {
        await this.container.items.upsert({id: hash, value: object.serialize()});
    }

    async saveDataArr(values: { id: string; object: T }[]): Promise<void> {
        let temp: UpsertOperationInput[] = [];
        for(let val of values) {
            temp.push({
                operationType: "Upsert",
                resourceBody: val.object.serialize(),
                partitionKey: val.id
            });
            if(temp.length>=100) {
                await this.container.items.bulk(temp);
                temp = [];
            }
        }
        if(temp.length>0) await this.container.items.bulk(temp);
    }

}
