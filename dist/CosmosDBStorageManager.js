"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CosmosDBStorageManager = void 0;
const CosmosDBBase_1 = require("./CosmosDBBase");
class CosmosDBStorageManager extends CosmosDBBase_1.CosmosDBBase {
    constructor(name, connectionString, databaseName = "Atomiq") {
        super(name, connectionString, databaseName);
        this.data = {};
    }
    async init() {
        await this.initDatabase();
        const { container } = await this.client.database(this.databaseName).containers.createIfNotExists({
            partitionKey: "/id",
            id: this.containerId,
            indexingPolicy: {
                indexingMode: "none"
            }
        });
        this.container = container;
    }
    async loadData(type) {
        const { resources } = await this.getContainer().items.readAll().fetchAll();
        this.data = {};
        const allData = [];
        resources.forEach(({ id, value }) => {
            const obj = new type(value);
            this.data[id] = obj;
            allData.push(obj);
        });
        return allData;
    }
    async removeData(hash) {
        await this.removeItem(hash);
    }
    async removeDataArr(keys) {
        await this.executeBulkOperations(keys.map(id => ({
            operationType: "Delete",
            id,
            partitionKey: id
        })), true);
    }
    async saveData(hash, object) {
        await this.getContainer().items.upsert({
            id: hash,
            value: object.serialize()
        });
    }
    async saveDataArr(values) {
        const { error } = await this.executeBulkOperations(values.map(val => ({
            operationType: "Upsert",
            resourceBody: {
                id: val.id,
                value: val.object.serialize()
            },
            partitionKey: val.id
        })));
        if (error != null)
            throw error;
    }
}
exports.CosmosDBStorageManager = CosmosDBStorageManager;
