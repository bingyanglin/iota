# IOTA gRPC API Crate (`iota-gprc-api`)

This crate provides a gRPC API interface for an IOTA node. It is currently under development as a **Proof of Concept (PoC)** with an initial focus on serving checkpoint data, object data, and transaction data.

## Purpose and Motivation

The primary motivation for this crate is to explore replacing the existing `iota-rest-api` for certain use cases, particularly for the `iota-indexer`.

Currently, `iota-indexer` syncs checkpoint data from an IOTA node using one of two methods:
1.  Polling a REST API endpoint.
2.  Reading checkpoint data directly from the filesystem.

This PoC aims to introduce a gRPC-based alternative that offers more efficient and reactive data synchronization through **gRPC subscriptions**. Specifically, `iota-indexer` could subscribe to a stream of new checkpoints directly from the node via this gRPC API, eliminating the need for polling and providing more timely updates.

The subscription logic for new checkpoints should draw inspiration from similar mechanisms in other IOTA components, such as the **INX interface in Hornet**. For an example of how Hornet's INX handles milestone (checkpoint) subscriptions, see:
[Hornet INX Server Milestones Subscription Logic](https://github.com/iotaledger/hornet/blob/3ab964191f30ec70f4d54dc121ea01bc48497bc1/components/inx/server_milestones.go#L169)

## Current Status

*   **Services Implemented (Using Real Data via MockRestStateReader in tests):**
    *   `CheckpointGprcService`:
        *   Unary RPCs: `GetCheckpoint`, `GetCheckpointFull`, `ListCheckpoints`.
        *   Server-streaming RPCs: `StreamCheckpointsInRange`, `SubscribeNewCheckpoints`.
            *   Both streaming RPCs now support an `include_full_data` flag to stream either `SignedCheckpointSummaryGprc` or full `CheckpointDataGprc`.
            *   `SubscribeNewCheckpoints` uses an internal pub/sub mechanism for reactive client updates, while the service itself polls the `state_reader`.
    *   `ObjectGprcService`:
        *   Unary RPCs: `GetObject`, `ListObjects`.
        *   Server-streaming RPCs: `StreamObjects` (lists current objects), `SubscribeObjectsByOwner` (subscribes to new/updated objects for an owner).
    *   `TransactionGprcService`:
        *   Unary RPCs: `GetTransaction`.
            *   `GetTransactionRequest` uses `bytes transaction_digest_bytes` for the ID.
*   **Proto Definitions:** Located in `src/proto/iota/gprc/v1/`.
*   **Build System:** `build.rs` compiles `.proto` files using `tonic-build`.
*   **Testing:** Unit tests for implemented services (`CheckpointGprcService`, `ObjectGprcService`, `TransactionGprcService`) are available in the `tests/` directory, running against a `MockRestStateReader`. All tests are currently passing.

## Getting Started

### Prerequisites

*   Rust toolchain
*   `protoc` (Protocol Buffer compiler):
    *   On macOS: `brew install protobuf`
    *   Other systems: Download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) and ensure it's in your `PATH`, or set the `PROTOC` environment variable.

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

## TODOs

*   **Integrate with Real Node State (Largely Complete for implemented services):**
    *   The `DummyStateReader` has been replaced with `StateReader` (an alias for `Arc<dyn iota_types::storage::RestStateReader>`).
    *   Implemented services (`CheckpointGprcService`, `ObjectGprcService`, `TransactionGprcService`) use the `state_reader` to fetch data.
    *   Conversions from `iota_types` to gRPC types are implemented for checkpoints, objects, and transactions.
    *   Unit tests use a `MockRestStateReader` which has been updated to support these calls, including dynamic updates for testing subscriptions.
    *   **Next Steps for Checkpoints:**
        *   The `include_full_data` flag is **complete** for `StreamCheckpointsInRange` and `SubscribeNewCheckpoints`.
        *   The `subscribe_new_checkpoints` RPC provides a true subscription for gRPC clients: clients connect once and receive a stream of new checkpoints as they are published by the server. Internally, to enable this, the `CheckpointServiceImpl` uses a reactive pub/sub model where a single background task polls the `state_reader` (the current interface to node data) and then broadcasts new checkpoints to all subscribed clients. This approach is a functional and efficient solution for client-side reactivity. Eliminating the service's internal polling would require the underlying node core (`StateReader` interface) to offer direct event notifications for new checkpoints.
*   **Implement Other gRPC Services (Transactions Service Started, Object Subscription Added):**
    *   `TransactionGprcService` (`TransactionServiceImpl`) has been implemented with the `GetTransaction` RPC.
        *   This RPC takes `bytes transaction_digest_bytes` in the request and converts `iota_types::transaction::VerifiedTransaction` to `TransactionGprc`.
        *   Unit tests for `GetTransaction` are implemented and pass using the `MockRestStateReader`.
    *   `ObjectGprcService` (`ObjectServiceImpl`) has a new `SubscribeObjectsByOwner` RPC for reactive updates.
        *   A dummy poller was used for testing the mechanism; a real event source or more sophisticated mock for object changes would be needed for full end-to-end testing of this RPC.
    *   **Next Steps:**
        *   Implement other RPCs for `TransactionGprcService` (e.g., `ListTransactions`, `StreamTransactions`).
        *   Further develop the `SubscribeObjectsByOwner` RPC, particularly integrating with a real event source for object changes if the node's state management provides it.
        *   Proceed to implement other services (committee, system, coins, epochs, accounts) with real data fetching and conversions.
*   **Configuration:** Add configuration options to enable/disable the gRPC API and set its listening address within the main IOTA node configuration (e.g., `validator.yaml`).
*   **Error Handling & Conversions (Ongoing):**
    *   Basic `GrpcApiError` and `From<GrpcApiError> for tonic::Status` implemented.
    *   Conversion functions are in `src/conversions/` for checkpoints, objects, and transactions.
    *   **Next Steps:**
        *   Expand error types and ensure comprehensive error handling across all services.
        *   Create and complete conversion modules for all necessary types for future services.
*   **Parity Testing:** Conduct thorough testing to ensure parity with the existing REST API where functionalities overlap, once services are implemented with real data.
*   **Integration with `iota-indexer`:** Modify `iota-indexer` to optionally use this gRPC API for checkpoint synchronization, particularly leveraging the `SubscribeNewCheckpoints` RPC.
