# IOTA gRPC API Crate (`iota-gprc-api`)

This crate provides a gRPC API interface for an IOTA node. It is currently under development as a **Proof of Concept (PoC)** with an initial focus on serving checkpoint data, object data, and transaction data.

## Purpose and Motivation

The primary motivation for this crate is to explore replacing the existing `iota-rest-api` for certain use cases, particularly for the `iota-indexer`.

Currently, `iota-indexer` syncs checkpoint data from an IOTA node using one of two methods:

1. Polling a REST API endpoint.
2. Reading checkpoint data directly from the filesystem.

This PoC aims to introduce a gRPC-based alternative that offers more efficient and reactive data synchronization through **gRPC subscriptions**. Specifically, `iota-indexer` could subscribe to a stream of new checkpoints directly from the node via this gRPC API, eliminating the need for polling and providing more timely updates.

The subscription logic for new checkpoints should draw inspiration from similar mechanisms in other IOTA components, such as the **INX interface in Hornet**. For an example of how Hornet's INX handles milestone (checkpoint) subscriptions, see:
[Hornet INX Server Milestones Subscription Logic](https://github.com/iotaledger/hornet/blob/3ab964191f30ec70f4d54dc121ea01bc48497bc1/components/inx/server_milestones.go#L169)

## Current Status

* **Services Implemented (Using Real Data via MockRestStateReader in tests):**
  * `CheckpointGprcService`:
    * Unary RPCs: `GetCheckpoint`, `GetCheckpointFull`, `ListCheckpoints`.
    * Server-streaming RPCs: `StreamCheckpointsInRange`, `SubscribeNewCheckpoints`.
      * Both streaming RPCs now support an `include_full_data` flag to stream either `SignedCheckpointSummaryGprc` or full `CheckpointDataGprc`.
      * `SubscribeNewCheckpoints` uses an internal pub/sub mechanism for reactive client updates, while the service itself polls the `state_reader`.
  * `ObjectGprcService`:
    * Unary RPCs: `GetObject`, `ListObjects`.
    * Server-streaming RPCs: `StreamObjects` (lists current objects).
  * `TransactionGprcService`:
    * Unary RPCs: `GetTransaction`.
      * `GetTransactionRequest` uses `bytes transaction_digest_bytes` for the ID.
* **Proto Definitions:** Located in `src/proto/iota/gprc/v1/`.
* **Build System:** `build.rs` compiles `.proto` files using `tonic-build`.
* **Testing:** Unit tests for implemented services (`CheckpointGprcService`, `ObjectGprcService`, `TransactionGprcService`) are available in the `tests/` directory, running against a `MockRestStateReader`. All tests are currently passing.

## Getting Started

### Prerequisites

* Rust toolchain
* `protoc` (Protocol Buffer compiler):
  * On macOS: `brew install protobuf`
  * Other systems: Download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) and ensure it's in your `PATH`, or set the `PROTOC` environment variable.

### Building the Crate

From the workspace root (`iota/`):

```bash
cargo build --release -p iota-gprc-api
```

Or, from within the crate's directory (`iota/crates/iota-gprc-api/`):

```bash
cargo build --release
```

### Running Tests

From the workspace root (`iota/`):

```bash
cargo test -p iota-gprc-api
```

Or, from within the crate's directory (`iota/crates/iota-gprc-api/`):

```bash
cargo test
```

## Configuration

The public gRPC API is configured within the main IOTA node settings. This is managed through the node's primary configuration file (e.g., `fullnode.yaml`), which corresponds to the `NodeConfig` struct in `crates/iota-config/src/node.rs`.

To enable and configure the gRPC API, you need to specify the `grpc_public_api_address` field in your node's configuration file:

```yaml
# Example part of a node configuration YAML
# ... other configurations ...

grpc_public_api_address: "127.0.0.1:9091" # Or your desired IP and port

# ... other configurations ...
```

**Details:**

* **Field:** `grpc_public_api_address: Option<SocketAddr>`
  * This field is defined in the `NodeConfig` Rust struct.
  * It accepts a string representing a socket address (IP address and port).
* **Enabling the Server:**
  * If `grpc_public_api_address` is provided with a valid socket address (e.g., `"0.0.0.0:9091"`, `"127.0.0.1:9091"`), the public gRPC server will be enabled and will start when the IOTA node initializes.
* **Disabling the Server:**
  * If the `grpc_public_api_address` field is omitted from the configuration file, or if its value is explicitly set to `null` (or not defined), the gRPC server will be disabled and will not start.
* **Node Integration:**
  * The IOTA node's startup logic checks this configuration value. If an address is present, it launches the gRPC server implemented in this (`iota-gprc-api`) crate.

Therefore, to use the public gRPC API, ensure the `grpc_public_api_address` is correctly set in your IOTA node's main configuration file.

## TODOs

* **Integrate with Real Node State (Largely Complete for implemented services):**
  * The `DummyStateReader` has been replaced with `StateReader` (an alias for `Arc<dyn iota_types::storage::RestStateReader>`).
  * Implemented services (`CheckpointGprcService`, `ObjectGprcService`, `TransactionGprcService`) use the `state_reader` to fetch data.
  * Conversions from `iota_types` to gRPC types are implemented for checkpoints, objects, and transactions.
  * Unit tests use a `MockRestStateReader` which has been updated to support these calls, including dynamic updates for testing subscriptions.
  * **Checkpoint Service Details:**
    * The `include_full_data` flag is **complete** for `StreamCheckpointsInRange` and `SubscribeNewCheckpoints`.
    * The `subscribe_new_checkpoints` RPC provides a true subscription for gRPC clients. Internally, `CheckpointServiceImpl` uses a reactive pub/sub model: a background task polls the `state_reader` and broadcasts new checkpoints. While this polling is a current design choice, eliminating it would require the underlying node core (`StateReader` interface) to offer direct event notifications.
* **Implement Other gRPC Services (Transactions Service Started, Object Subscription Added):**
  * `TransactionGprcService` (`TransactionServiceImpl`) has been implemented with the `GetTransaction` RPC.
    * This RPC takes `bytes transaction_digest_bytes` in the request and converts `iota_types::transaction::VerifiedTransaction` to `TransactionGprc`.
    * Unit tests for `GetTransaction` are implemented and pass using the `MockRestStateReader`.
  * `ObjectGprcService` (`ObjectServiceImpl`) has a new `SubscribeObjectsByOwner` RPC for reactive updates.
    * A dummy poller was used for testing the mechanism; a real event source or more sophisticated mock for object changes would be needed for full end-to-end testing of this RPC.

  * **Current Implementations (Continued from above & new):**
    * `TransactionGprcService`:
      * `GetTransaction` RPC is implemented (as mentioned under "Implement Other gRPC Services").
      * `ListTransactions` and `StreamTransactions` RPCs are implemented and use the `state_reader` to fetch and stream transaction data. (Unit tests use a `MockRestStateReader`).
    * `CommitteeGprcService`:
      * `GetCommittee` RPC is implemented and uses the `state_reader`.
      * `StreamCommittee` RPC is implemented and uses the `state_reader` with a polling mechanism.
    * `SystemGprcService` (`SystemServiceImpl`):
      * `GetSystemInfo` RPC is implemented:
        * Returns node version (currently placeholder) and uptime.
        * Tested.
      * `SubscribeSystemEvents` RPC is implemented:
        * Streams mock `NodeStatusChanged` events periodically.
        * Tested.
    * `CoinsGprcService` (`CoinsServiceImpl`):
      * `GetCoinInfo` RPC is implemented:
        * Takes a `coin_type_tag` string.
        * Fetches `iota_types::storage::CoinInfo` and attempts to resolve the `treasury_object_id` to a `TreasuryCap` object to retrieve `total_supply`.
        * Does not currently populate `CoinMetadata` details (like name, symbol, decimals).
        * Tested (success, not found, invalid tag).
      * `ListCoins` RPC remains a stub (returns `Unimplemented`).
      * `SubscribeCoinEvents` RPC remains a stub (returns `Unimplemented`).
    * Other Services (`EpochsGprcService`, `AccountsGprcService`):
      * Basic stub implementations are in place.
    * Error Handling & Conversions:
      * Basic `GrpcApiError` and `From<GrpcApiError> for tonic::Status` implemented.
      * Conversion functions are in `src/conversions/` for checkpoints, objects, and transactions.

  * **Next Steps & Ongoing Development:**
    * `ObjectGprcService`:
      * Further develop the `StreamObjects` RPC.
    * `CoinsGprcService`:
      * Implement `ListCoins` RPC.
      * Enhance `GetCoinInfo` to also fetch and convert `CoinMetadata` (name, symbol, decimals, etc.) using the `coin_metadata_object_id` from `iota_types::storage::CoinInfo`.
    * `SystemGprcService`:
      * Integrate `GetSystemInfo` with actual node build version and potentially other system metrics from the `state_reader` if available.
    * Other Services (`EpochsGprcService`, `AccountsGprcService`):
      * Implement the actual logic for data fetching and conversions for their respective RPCs.
    * Error Handling & Conversions:
      * Expand error types and ensure comprehensive error handling across all services.
      * Create and complete conversion modules for all necessary types for future services.

* **Parity Testing:** Conduct thorough testing to ensure parity with the existing REST API where functionalities overlap, once services are implemented with real data.
* **Integration with `iota-indexer`:** Modify `iota-indexer` to optionally use this gRPC API for checkpoint synchronization, particularly leveraging the `SubscribeNewCheckpoints` RPC.
* **Add Copyright**
