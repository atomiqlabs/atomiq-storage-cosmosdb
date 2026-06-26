# @atomiqlabs/storage-cosmosdb

`@atomiqlabs/storage-cosmosdb` is the Azure Cosmos DB-backed storage adapter for the Atomiq SDK in Node.js backend environments. The SDK uses browser IndexedDB by default. Backends, workers, and shared services need to provide storage implementations explicitly. This package provides those implementations on top of Azure Cosmos DB for NoSQL containers.

## What this package provides

- `CosmosDBSwapStorage`: persistent unified swap storage used by the SDK for swap records and indexed queries.
- `CosmosDBSwapPatchStorage`: persistent unified swap storage that writes Cosmos DB patch operations for existing swap records when possible.
- `CosmosDBStorageManager`: persistent storage manager used for chain-specific SDK stores.
- `CosmosDBConcurrencyError`: error thrown when a Cosmos DB ETag-based concurrency check fails.

Each storage instance uses one Cosmos DB container. Constructors take the container id, the Cosmos DB connection string, and an optional database name. The database name defaults to `Atomiq`.

## When to use it

Use this package when you run the Atomiq SDK in:

- Node.js backend services
- backend workers
- Azure-specific runtimes such as Azure Functions or other serverless function apps
- server-side deployments that need shared cloud persistence
- environments where local SQLite files are not suitable

Do not use this package from browser or React Native apps, because the Cosmos DB connection string must stay server-side. Browser apps usually use the SDK's default IndexedDB storage, and React Native apps can use `@atomiqlabs/storage-rn-async`.

## Installation

```bash
npm install @atomiqlabs/sdk @atomiqlabs/base @atomiqlabs/storage-cosmosdb
```

This adapter uses `@azure/cosmos` under the hood. Your application needs access to an Azure Cosmos DB for NoSQL account and a connection string for that account.

## Cosmos DB setup

During SDK initialization, the adapter creates the configured database and containers if they do not already exist.

- Swap storage containers use partition key `/id` and an indexing policy generated from the SDK's simple and composite swap indexes.
- Chain storage manager containers use partition key `/id` with indexing disabled.
- If you pre-create containers, configure them with the same partition key and compatible indexing policy.
- Use stable container ids for each swap chain and chain-specific store. Changing a container id points the SDK at a different storage namespace.

## SDK Usage

Pass `CosmosDBSwapStorage` as `swapStorage` and `CosmosDBStorageManager` as `chainStorageCtor` when creating the swapper.

```typescript
import {BitcoinNetwork, SwapperFactory, TypedSwapper} from "@atomiqlabs/sdk";
import {CosmosDBStorageManager, CosmosDBSwapStorage} from "@atomiqlabs/storage-cosmosdb";

const connectionString = process.env.ATOMIQ_COSMOSDB_CONNECTION_STRING;
if(connectionString == null) throw new Error("Missing ATOMIQ_COSMOSDB_CONNECTION_STRING");

const chains = [SolanaInitializer, StarknetInitializer, CitreaInitializer] as const;
type SupportedChains = typeof chains;

const Factory = new SwapperFactory<SupportedChains>(chains);

const swapper: TypedSwapper<SupportedChains> = Factory.newSwapper({
    chains: {
        ...
    },
    bitcoinNetwork: BitcoinNetwork.MAINNET,
    // In Node.js, provide persistent storage because the SDK's default
    // browser storage implementation is IndexedDB.
    swapStorage: chainId => new CosmosDBSwapStorage(
        `ATQ_SWAPS_${chainId}`,
        connectionString
    ),
    chainStorageCtor: name => new CosmosDBStorageManager(
        `ATQ_STORE_${name}`,
        connectionString
    )
});

await swapper.init();
```

To use a custom database name, pass it as the third constructor argument:

```typescript
swapStorage: chainId => new CosmosDBSwapStorage(
    `ATQ_SWAPS_${chainId}`,
    connectionString,
    "MyAtomiqDatabase"
)
```

## Patch-based swap storage

`CosmosDBSwapPatchStorage` has the same constructor shape as `CosmosDBSwapStorage`, plus an optional fourth `optimisticConcurrency` argument.

```typescript
import {CosmosDBSwapPatchStorage} from "@atomiqlabs/storage-cosmosdb";

swapStorage: chainId => new CosmosDBSwapPatchStorage(
    `ATQ_SWAPS_${chainId}`,
    connectionString,
    "Atomiq",
    true
)
```

Use `CosmosDBSwapPatchStorage` when you want existing swap updates to be sent as Cosmos DB patch operations where possible. When `optimisticConcurrency` is `true`, patch, replace, and delete operations for previously loaded swaps include the loaded ETag and throw `CosmosDBConcurrencyError` if the document changed concurrently. The default is `false`.

## Notes

- `CosmosDBSwapStorage` stores one swap per Cosmos DB item and uses Cosmos DB indexes generated from the SDK-provided storage schema.
- Swap containers and chain storage containers are separate. Use distinct, stable container ids for `swapStorage` and `chainStorageCtor`.
- `CosmosDBConcurrencyError` includes the affected `itemIds` so callers can decide whether to retry, reload, or surface the conflict.
- Persistence, availability, throughput, and backup behavior depend on the Azure Cosmos DB account configuration you choose.
