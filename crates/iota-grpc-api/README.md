# IOTA gRPC API Crate (`iota-grpc-api`)

This crate provides a gRPC API interface for an IOTA node. It is currently under development as a **Proof of Concept (PoC)** with the **sole focus** on serving checkpoint data.

## Purpose and Motivation

The primary motivation for this crate is to explore replacing the existing `iota-rest-api` for certain use cases, particularly for the `iota-indexer`.

Currently, `iota-indexer` syncs checkpoint data from an IOTA node using one of two methods:
1.  Polling a REST API endpoint.
2.  Reading checkpoint data directly from the filesystem.

This PoC aims to introduce a gRPC-based alternative that offers more efficient and reactive data synchronization through **gRPC subscriptions**. Specifically, `iota-indexer` could subscribe to a stream of new checkpoints directly from the node via this gRPC API, eliminating the need for polling and providing more timely updates.

The subscription logic for new checkpoints should draw inspiration from similar mechanisms in other IOTA components, such as the **INX interface in Hornet**. For an example of how Hornet's INX handles milestone (checkpoint) subscriptions, see:
[Hornet INX Server Milestones Subscription Logic](https://github.com/iotaledger/hornet/blob/3ab964191f30ec70f4d54dc121ea01bc48497bc1/components/inx/server_milestones.go#L169)

## Current Status

*   **Services Implemented (for checkpoints, using MockRestStateReader in tests):**
    *   `CheckpointGprcService`:
        *   Unary RPCs: `GetCheckpoint`, `GetCheckpointFull`, `ListCheckpoints`.
        *   Server-streaming RPCs: `SubscribeNewCheckpoints`.
            *   Both streaming RPCs now support an `include_full_data` flag to stream either `SignedCheckpointSummaryGprc` or full `CheckpointDataGprc`.
            *   `SubscribeNewCheckpoints` uses an internal pub/sub mechanism for reactive client updates, while the service itself polls the `state_reader`.
*   **Proto Definitions:** Located in `src/proto/iota/gprc/v1/`.
*   **Build System:** `build.rs` compiles `.proto` files using `tonic-build`.
*   **Testing:** Unit tests for the `CheckpointGprcService` are available in the `tests/` directory, running against a `MockRestStateReader`. All tests are currently passing.

## Getting Started

### Prerequisites

*   Rust toolchain
*   `protoc` (Protocol Buffer compiler):
    *   On macOS: `brew install protobuf`
    *   Other systems: Download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) and ensure it's in your `PATH`, or set the `PROTOC` environment variable.

### Building the Crate

From the workspace root (`iota/`):

```bash
cargo build --release -p iota-grpc-api
```

Or, from within the crate's directory (`iota/crates/iota-grpc-api/`):

```bash
cargo build --release
```

### Running Tests

From the workspace root (`iota/`):

```bash
cargo test -p iota-indexer                                   
cargo test -p iota-data-ingestion-core            
cargo test -p iota-data-ingestion
cargo test -p iota-grpc-api
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

*   **Field:** `grpc_public_api_address: Option<SocketAddr>`
    *   This field is defined in the `NodeConfig` Rust struct.
    *   It accepts a string representing a socket address (IP address and port).
*   **Enabling the Server:**
    *   If `grpc_public_api_address` is provided with a valid socket address (e.g., `"0.0.0.0:9091"`, `"127.0.0.1:9091"`), the public gRPC server will be enabled and will start when the IOTA node initializes.
*   **Disabling the Server:**
    *   If the `grpc_public_api_address` field is omitted from the configuration file, or if its value is explicitly set to `null` (or not defined), the gRPC server will be disabled and will not start.
*   **Node Integration:**
    *   The IOTA node's startup logic checks this configuration value. If an address is present, it launches the gRPC server implemented in this (`iota-grpc-api`) crate.

Therefore, to use the public gRPC API, ensure the `grpc_public_api_address` is correctly set in your IOTA node's main configuration file.

## TODOs

*   **Integrate `CheckpointGprcService` with Real Node State (Largely Complete):**
    *   The `DummyStateReader` has been replaced with `StateReader` (an alias for `Arc<dyn iota_types::storage::RestStateReader>`).
    *   The `CheckpointGprcService` uses the `state_reader` to fetch data.
    *   Conversions from `iota_types` to gRPC types are implemented for checkpoints.
    *   Unit tests use a `MockRestStateReader` which has been updated to support these calls, including dynamic updates for testing subscriptions.
    *   **Checkpoint Service Details:**
        *   The `include_full_data` flag is **complete** for `SubscribeNewCheckpoints`.
        *   The `subscribe_new_checkpoints` RPC provides a true subscription for gRPC clients. Internally, `CheckpointServiceImpl` uses a reactive pub/sub model: a background task polls the `state_reader` and broadcasts new checkpoints. While this polling is a current design choice, eliminating it would require the underlying node core (`StateReader` interface) to offer direct event notifications.
*   **Next Steps & Ongoing Development:**
    *   Error Handling & Conversions: Expand error types and ensure comprehensive error handling across the `CheckpointGprcService`. Create and complete conversion modules for all necessary types for checkpoint-related data.
*   **Parity Testing:** Conduct thorough testing to ensure parity with the existing REST API for checkpoint fetching where functionalities overlap.
*   **Integration with `iota-indexer`:** Modify `iota-indexer` to optionally use this gRPC API for checkpoint synchronization, particularly leveraging the `SubscribeNewCheckpoints` RPC.
*   **Add Copyright**
