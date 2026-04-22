import {Container, CosmosClient, DeleteOperationInput, PatchOperation, UpsertOperationInput} from "@azure/cosmos";
import {IUnifiedStorage, QueryParams, UnifiedStoredObject, UnifiedSwapStorageCompositeIndexes, UnifiedSwapStorageIndexes} from "@atomiqlabs/sdk";

export class CosmosDBSwapStorage implements IUnifiedStorage<UnifiedSwapStorageIndexes, UnifiedSwapStorageCompositeIndexes> {

    client: CosmosClient;
    chainId: string;
    container: Container;

    constructor(chainId: string) {
        this.chainId = chainId;
        this.client = new CosmosClient(process.env.COSMOS_CONECTION_STRING);
    }

    async init(): Promise<void> {
        const {container} = await this.client.database("Atomiq").containers.createIfNotExists({
            partitionKey: "/id",
            id: this.chainId
        });
        this.container = container;
    }

    async query(params: Array<Array<QueryParams>>): Promise<Array<UnifiedStoredObject>> {
        const orQuery: string[] = [];
        const values: {name: string, value: any}[] = [];

        let counter = 0;
        for(let orParams of params) {
            const andQuery: string[] = [];
            for(let andParam of orParams) {
                const tag = "@"+andParam.key+counter.toString(10).padStart(8, "0");
                if(Array.isArray(andParam.value)) {
                    andQuery.push("ARRAY_CONTAINS("+tag+", c."+andParam.key+")");
                } else {
                    andQuery.push("c."+andParam.key+" = "+tag);
                }
                values.push({
                    name: tag,
                    value: andParam.value
                });
                counter++;
            }
            orQuery.push("("+andQuery.join(" AND ")+")");
        }

        const queryToSend = "SELECT * FROM c WHERE "+orQuery.join(" OR ");
        console.log("Sending query: "+queryToSend);
        console.log("Parameters: ", values);

        const {resources} = await this.container.items.query<UnifiedStoredObject>({
            query: queryToSend,
            parameters: values
        }).fetchAll();

        console.log("Query result: ", resources);

        return resources;
    }

    async remove(value: UnifiedStoredObject): Promise<void> {
        await this.container.item(value.id, value.id).delete();
    }

    async removeAll(value: UnifiedStoredObject[]): Promise<void> {
        let temp: DeleteOperationInput[] = [];
        for(let val of value) {
            temp.push({
                operationType: "Delete",
                id: val.id,
                partitionKey: val.id
            });
            if(temp.length>=100) {
                await this.container.items.bulk(temp, {continueOnError: true});
                temp = [];
            }
        }
        if(temp.length>0) await this.container.items.bulk(temp);
    }

    async save(value: UnifiedStoredObject): Promise<void> {
        await this.container.items.upsert(value);
    }

    async saveAll(value: UnifiedStoredObject[]): Promise<void> {
        let temp: UpsertOperationInput[] = [];
        for(let val of value) {
            temp.push({
                operationType: "Upsert",
                resourceBody: val,
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
