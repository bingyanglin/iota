# IOTA gRPC API Crate (`iota-grpc-api`)

This crate provides a gRPC API interface for an IOTA node. It is currently under development as a **Proof of Concept (PoC)** with the **sole focus** on serving checkpoint data via an event-driven, server-driven streaming mechanism.

## Purpose and Motivation

The primary motivation for this crate is to explore replacing the existing `iota-rest-api` for certain use cases, particularly for the `iota-indexer`'s checkpoint synchronization.

Currently, `iota-indexer` syncs checkpoint data from an IOTA node using one of two methods:
1.  Polling a REST API endpoint.
2.  Reading checkpoint data directly from the filesystem.

This PoC introduces a gRPC-based alternative that offers more efficient and reactive data synchronization. The `iota-indexer` can subscribe to a stream of checkpoints (`SubscribeNewCheckpoints` RPC) directly from the node via this gRPC API. **The server is responsible for sending both historical checkpoints (if the client is behind) and live, event-driven updates on the same stream.** This model is inspired by the [INX interface in Hornet](https://github.com/iotaledger/hornet/blob/3ab964191f30ec70f4d54dc121ea01bc48497bc1/components/inx/server_milestones.go#L169), where the client simply subscribes and receives all relevant data in order, without needing to perform separate catch-up calls.

## Current Status

*   **Services Implemented (for checkpoints):**
    *   `CheckpointGprcService` defines the following RPCs:
        *   Unary RPCs: `GetCheckpoint`, `GetCheckpointFull`, `ListCheckpoints` (for direct access if needed).
        *   Server-streaming RPC: `SubscribeNewCheckpoints`.
            *   `SubscribeNewCheckpoints` is fully event-driven and server-driven: upon subscription, the server first sends any historical checkpoints (from the requested sequence number up to the latest), then seamlessly transitions to streaming new, live checkpoints as they are finalized.
            *   The stream supports an `include_full_data` flag to receive either `SignedCheckpointSummaryGprc` or full `CheckpointDataGprc`.
*   **Proto Definitions:** Located in `src/proto/iota/gprc/v1/`.
*   **Build System:** `build.rs` compiles `.proto` files using `tonic-build`.
*   **Testing:** Unit and integration tests validate both the server-driven catch-up and live streaming, including reconnection and end-to-end ingestion with `iota-indexer`.

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
cargo test -p iota-grpc-api
# For integration tests demonstrating client usage:
cargo test -p iota-indexer --test grpc_streaming_integration_test
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

## TODOs (Post-PoC / Future Considerations)

*   **Real Node Integration:** Fully integrate `CheckpointGprcService` with a real IOTA node's state and event system to enable true event-driven checkpoint streaming from the node core. This would involve adapting the `StateReader` interface or introducing a new mechanism for the node to push checkpoint events directly to the gRPC service.
*   **Error Handling & Conversions:** Continue to refine error types and ensure comprehensive error handling across the `CheckpointGprcService`. Complete any remaining conversion logic for checkpoint-related data if new fields or types are introduced.
*   **Parity Testing:** Conduct thorough testing to ensure parity with the existing REST API for checkpoint fetching where functionalities overlap, especially if this gRPC service were to replace parts of it.
*   **Production Hardening of `iota-indexer` Integration:** The core integration demonstrating `iota-indexer`'s use of this gRPC API for event-driven streaming and efficient historical catch-up is complete within this PoC. Further refinements, comprehensive error handling, and performance optimizations would be needed for a production-ready system.
*   **COPYRIGHT:** Add copyright headers to all files.