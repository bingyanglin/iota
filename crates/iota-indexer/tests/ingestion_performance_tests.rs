// Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use iota_indexer::{
    store::indexer_store::IndexerStore,
    test_utils::{IndexerTypeConfig, start_test_indexer, start_test_indexer_grpc},
};
use test_cluster::TestClusterBuilder;
use tokio_util::sync::CancellationToken;

const START_CP: u64 = 0;
const MAX_CP: u64 = 4999;

/// Profiler for tracking checkpoint ingestion latencies
pub struct IngestionLatencyProfiler {
    pub checkpoint_creation_times: HashMap<u64, Instant>,
    pub checkpoint_indexed_times: HashMap<u64, Instant>,
    pub method_name: String,
}

impl IngestionLatencyProfiler {
    pub fn new(method_name: &str) -> Self {
        Self {
            checkpoint_creation_times: HashMap::new(),
            checkpoint_indexed_times: HashMap::new(),
            method_name: method_name.to_string(),
        }
    }

    pub fn record_checkpoint_created(&mut self, seq: u64) {
        self.checkpoint_creation_times.insert(seq, Instant::now());
    }

    pub fn record_checkpoint_indexed(&mut self, seq: u64) {
        self.checkpoint_indexed_times.insert(seq, Instant::now());
    }

    pub fn calculate_latencies(&self) -> Vec<Duration> {
        self.checkpoint_creation_times
            .iter()
            .filter_map(|(&seq, &creation_time)| {
                self.checkpoint_indexed_times
                    .get(&seq)
                    .map(|&indexed_time| indexed_time.duration_since(creation_time))
            })
            .collect()
    }

    pub fn print_statistics(&self) {
        let latencies = self.calculate_latencies();
        if latencies.is_empty() {
            println!("[{}] No latency data available", self.method_name);
            return;
        }

        let mut sorted_latencies = latencies.clone();
        sorted_latencies.sort();

        let min = sorted_latencies.first().unwrap();
        let max = sorted_latencies.last().unwrap();
        let avg = Duration::from_nanos(
            (sorted_latencies.iter().map(|d| d.as_nanos()).sum::<u128>()
                / sorted_latencies.len() as u128) as u64,
        );

        let median = sorted_latencies[sorted_latencies.len() / 2];

        println!(
            "\n=== {} Ingestion Latency Statistics ===",
            self.method_name
        );
        println!("Samples: {}", sorted_latencies.len());
        println!("Min:     {:?}", min);
        println!("Max:     {:?}", max);
        println!("Average: {:?}", avg);
        println!("Median:  {:?}", median);
        println!("==========================================\n");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_sync_performance_rest() {
    // Start a test cluster
    let cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;

    // Wait for a range of checkpoints to be available
    cluster.wait_for_checkpoint(MAX_CP, None).await;

    // Prepare DB and indexer
    let db_url = "postgres://postgres:postgrespw@localhost:5432/iota_indexer_perf_rest".to_string();
    let rpc_url = cluster.rpc_url().to_string();
    let (store, handle) = start_test_indexer(
        db_url.clone(),
        true,
        None,
        rpc_url,
        IndexerTypeConfig::writer_mode(None, None),
        None,
    )
    .await;

    // Wait for the indexer to process up to MAX_CP
    let t0 = Instant::now();
    tokio::time::timeout(std::time::Duration::from_secs(3600), async {
        loop {
            if let Ok((_min_cp, max_cp)) = store.get_available_checkpoint_range().await {
                if max_cp >= MAX_CP {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Indexer did not process checkpoints in time");
    let elapsed = t0.elapsed();
    println!(
        "[REST] Synced checkpoints {}-{} in {:?}",
        START_CP, MAX_CP, elapsed
    );

    // Clean up
    handle.abort();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_sync_performance_grpc() {
    // Start a test cluster with gRPC enabled
    let grpc_port = 50052u16;
    let grpc_addr = format!("127.0.0.1:{}", grpc_port);
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.clone())
        .with_num_validators(1)
        .build()
        .await;

    // Wait for a range of checkpoints to be available
    cluster.wait_for_checkpoint(MAX_CP, None).await;

    // Prepare DB and indexer
    let db_url = "postgres://postgres:postgrespw@localhost:5432/iota_indexer_perf_grpc".to_string();
    let cancel = CancellationToken::new();
    let (store, handle) = start_test_indexer_grpc(
        db_url.clone(),
        true,
        None,
        format!("http://{}", grpc_addr),
        None,
        cancel.clone(),
    )
    .await;

    // Wait for the indexer to process up to MAX_CP
    let t0 = Instant::now();
    tokio::time::timeout(std::time::Duration::from_secs(3600), async {
        loop {
            if let Ok((_min_cp, max_cp)) = store.get_available_checkpoint_range().await {
                if max_cp >= MAX_CP {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Indexer did not process checkpoints in time");
    let elapsed = t0.elapsed();
    println!(
        "[gRPC] Synced checkpoints {}-{} in {:?}",
        START_CP, MAX_CP, elapsed
    );

    // Clean up
    cancel.cancel();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_sync_performance_file() {
    use iota_indexer::store::indexer_store::IndexerStore;
    use tempfile::tempdir;

    // Start a test cluster with gRPC enabled (needed for checkpoint export)
    let grpc_port = 50059u16;
    let grpc_addr = format!("127.0.0.1:{}", grpc_port);
    let cluster = TestClusterBuilder::new()
        .with_fullnode_grpc_api_address(grpc_addr.clone())
        .with_num_validators(1)
        .build()
        .await;

    // Wait for a range of checkpoints to be available
    cluster.wait_for_checkpoint(MAX_CP, None).await;

    // Export checkpoints to a temp directory
    let temp_dir = tempdir().unwrap();
    let checkpoint_dir = temp_dir.path().to_path_buf();

    // --- Generate and export checkpoints via gRPC FIRST ---
    {
        use futures::StreamExt;
        use iota_grpc_api::client::GrpcNodeClient;
        use iota_storage::blob::{Blob, BlobEncoding};
        use iota_types::full_checkpoint_content::CheckpointData;

        // Use the configured gRPC address
        let grpc_url = format!("http://{}", grpc_addr);
        let mut grpc_client = GrpcNodeClient::connect(&grpc_url)
            .await
            .expect("connect gRPC");
        let mut stream = grpc_client
            .stream_checkpoints(Some(START_CP), Some(MAX_CP), Some(true))
            .await
            .expect("gRPC stream");
        while let Some(Ok(cp)) = stream.next().await {
            // Deserialize the BCS data into CheckpointData
            let checkpoint_data: CheckpointData = GrpcNodeClient::deserialize_checkpoint_data(&cp)
                .expect("deserialize checkpoint data");

            // Re-encode using Blob format (which is what the file reader expects)
            let blob = Blob::encode(&checkpoint_data, BlobEncoding::Bcs)
                .expect("encode checkpoint as blob");

            let file_path = checkpoint_dir.join(format!("{}.chk", cp.index));
            std::fs::write(file_path, blob.to_bytes()).expect("write checkpoint file");
        }
        println!(
            "Exported {} checkpoint files to {:?}",
            MAX_CP - START_CP + 1,
            checkpoint_dir
        );
    }
    // --- End export ---

    // Now start the indexer with the populated checkpoint directory
    let db_url = "postgres://postgres:postgrespw@localhost:5432/iota_indexer_perf_file".to_string();
    let (store, handle) = iota_indexer::test_utils::start_test_indexer(
        db_url.clone(),
        true,
        None,
        cluster.rpc_url().to_string(),
        IndexerTypeConfig::writer_mode(None, None),
        Some(checkpoint_dir.clone()),
    )
    .await;

    // Wait for the indexer to process up to MAX_CP
    let t0 = Instant::now();
    tokio::time::timeout(std::time::Duration::from_secs(3600), async {
        loop {
            if let Ok((_min_cp, max_cp)) = store.get_available_checkpoint_range().await {
                if max_cp >= MAX_CP {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Indexer did not process checkpoints in time");
    let elapsed = t0.elapsed();
    println!(
        "[File] Synced checkpoints {}-{} in {:?}",
        START_CP, MAX_CP, elapsed
    );

    // Clean up
    handle.abort();
    let _ = handle.await;
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// async fn test_rest_ingestion_latency_profiling() {
//     use iota_test_transaction_builder::make_transfer_iota_transaction;
//     use iota_types::base_types::IotaAddress;

//     let mut profiler = IngestionLatencyProfiler::new("REST");
//     let num_samples = 20u64;
//     println!(
//         "[REST] Starting REAL-TIME latency profiling test with {} samples",
//         num_samples
//     );

//     // Start a basic test cluster
//     let cluster = TestClusterBuilder::new()
//         .with_num_validators(1)
//         .build()
//         .await;

//     println!("[REST] Cluster started, starting indexer FIRST...");

//     // Start indexer BEFORE generating new checkpoints to measure real-time
//     // ingestion
//     let db_url =
// "postgres://postgres:postgrespw@localhost:5432/
// iota_indexer_latency_rest_realtime"         .to_string();

//     let (store, handle) = start_test_indexer(
//         db_url.clone(),
//         true,
//         None,
//         cluster.rpc_url().to_string(),
//         IndexerTypeConfig::writer_mode(None, None),
//         None, // No file path for REST
//     )
//     .await;

//     println!("[REST] Indexer started and catching up to current state...");

//     // Wait for indexer to catch up to current state
//     tokio::time::sleep(std::time::Duration::from_secs(2)).await;

//     // Get the current checkpoint sequence number before we start creating
// new ones     let baseline_checkpoint = store
//         .get_latest_checkpoint_sequence_number()
//         .await
//         .unwrap()
//         .unwrap_or(0);
//     println!(
//         "[REST] Baseline checkpoint: {}, starting real-time measurements...",
//         baseline_checkpoint
//     );

//     // Now create new transactions and measure real-time ingestion latency
//     for i in 1..=num_samples {
//         println!("[REST] Creating new transaction {} of {}", i, num_samples);

//         let recipient = IotaAddress::random_for_testing_only();
//         let tx = make_transfer_iota_transaction(&cluster.wallet,
// Some(recipient), None).await;

//         // Record the time just before executing the transaction
//         // let tx_submit_time = std::time::Instant::now();
//         let _response = cluster.execute_transaction(tx).await;

//         // Wait for the cluster to create a new checkpoint containing this
// transaction         let expected_checkpoint = baseline_checkpoint + i;
//         cluster.wait_for_checkpoint(expected_checkpoint, None).await;

//         // Record when the checkpoint was created
//         profiler.record_checkpoint_created(expected_checkpoint);

//         println!(
//             "[REST] Checkpoint {} created, waiting for indexer to process
// it...",             expected_checkpoint
//         );

//         // Wait for the indexer to process this checkpoint
//         let timeout_duration = std::time::Duration::from_secs(10);
//         tokio::time::timeout(timeout_duration, async {
//             loop {
//                 if let Ok(latest_seq) =
// store.get_latest_checkpoint_sequence_number().await {                     if
// let Some(seq) = latest_seq {                         if seq >=
// expected_checkpoint {
// profiler.record_checkpoint_indexed(expected_checkpoint);
// println!(                                 "[REST] Checkpoint {} now available
// in indexer",                                 expected_checkpoint
//                             );
//                             break;
//                         }
//                     }
//                 }
//
// tokio::time::sleep(std::time::Duration::from_millis(10)).await;             }
//         })
//         .await
//         .expect(&format!(
//             "Indexer did not process checkpoint {} within timeout",
//             expected_checkpoint
//         ));

//         // Small delay between transactions to avoid overwhelming the system
//         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//     }

//     println!(
//         "[REST] Completed {} real-time latency measurements",
//         num_samples
//     );

//     // Print detailed latency statistics
//     profiler.print_statistics();

//     // Clean up
//     handle.abort();
//     let _ = handle.await;
//     println!("[REST] Real-time latency test completed successfully");
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// async fn test_grpc_ingestion_latency_profiling() {
//     use iota_test_transaction_builder::make_transfer_iota_transaction;
//     use iota_types::base_types::IotaAddress;

//     let mut profiler = IngestionLatencyProfiler::new("gRPC");
//     let num_samples = 20u64;
//     println!(
//         "[gRPC] Starting REAL-TIME latency profiling test with {} samples",
//         num_samples
//     );

//     // Start a test cluster with gRPC enabled
//     let grpc_port = 50053u16;
//     let grpc_addr = format!("127.0.0.1:{}", grpc_port);
//     let cluster = TestClusterBuilder::new()
//         .with_fullnode_grpc_api_address(grpc_addr.clone())
//         .with_num_validators(1)
//         .build()
//         .await;

//     println!("[gRPC] Cluster started with gRPC, starting indexer FIRST...");

//     // Start indexer with gRPC ingestion BEFORE generating new checkpoints
//     let db_url =
// "postgres://postgres:postgrespw@localhost:5432/
// iota_indexer_latency_grpc_realtime"         .to_string();
//     let grpc_url = format!("http://{}", grpc_addr);

//     let cancel = CancellationToken::new();
//     let (store, handle) = start_test_indexer_grpc(
//         db_url.clone(),
//         true,
//         None,
//         grpc_url,
//         None, // data_ingestion_path
//         cancel.clone(),
//     )
//     .await;

//     println!("[gRPC] Indexer started and catching up to current state...");

//     // Wait for indexer to catch up to current state
//     tokio::time::sleep(std::time::Duration::from_secs(2)).await;

//     // Get the current checkpoint sequence number before we start creating
// new ones     let baseline_checkpoint = store
//         .get_latest_checkpoint_sequence_number()
//         .await
//         .unwrap()
//         .unwrap_or(0);
//     println!(
//         "[gRPC] Baseline checkpoint: {}, starting real-time measurements...",
//         baseline_checkpoint
//     );

//     // Now create new transactions and measure real-time ingestion latency
//     for i in 1..=num_samples {
//         println!("[gRPC] Creating new transaction {} of {}", i, num_samples);

//         let recipient = IotaAddress::random_for_testing_only();
//         let tx = make_transfer_iota_transaction(&cluster.wallet,
// Some(recipient), None).await;

//         // Record the time just before executing the transaction
//         // let tx_submit_time = std::time::Instant::now();
//         let _response = cluster.execute_transaction(tx).await;

//         // Wait for the cluster to create a new checkpoint containing this
// transaction         let expected_checkpoint = baseline_checkpoint + i;
//         cluster.wait_for_checkpoint(expected_checkpoint, None).await;

//         // Record when the checkpoint was created
//         profiler.record_checkpoint_created(expected_checkpoint);

//         println!(
//             "[gRPC] Checkpoint {} created, waiting for indexer to process
// it...",             expected_checkpoint
//         );

//         // Wait for the indexer to process this checkpoint
//         let timeout_duration = std::time::Duration::from_secs(10);
//         tokio::time::timeout(timeout_duration, async {
//             loop {
//                 if let Ok(latest_seq) =
// store.get_latest_checkpoint_sequence_number().await {                     if
// let Some(seq) = latest_seq {                         if seq >=
// expected_checkpoint {
// profiler.record_checkpoint_indexed(expected_checkpoint);
// println!(                                 "[gRPC] Checkpoint {} now available
// in indexer",                                 expected_checkpoint
//                             );
//                             break;
//                         }
//                     }
//                 }
//
// tokio::time::sleep(std::time::Duration::from_millis(10)).await;             }
//         })
//         .await
//         .expect(&format!(
//             "Indexer did not process checkpoint {} within timeout",
//             expected_checkpoint
//         ));

//         // Small delay between transactions to avoid overwhelming the system
//         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//     }

//     println!(
//         "[gRPC] Completed {} real-time latency measurements",
//         num_samples
//     );

//     // Print detailed latency statistics
//     profiler.print_statistics();

//     // Clean up
//     cancel.cancel();
//     let _ = handle.await;
//     println!("[gRPC] Real-time latency test completed successfully");
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// async fn test_file_ingestion_latency_profiling() {
//     use iota_storage::blob::{Blob, BlobEncoding};
//     use iota_test_transaction_builder::make_transfer_iota_transaction;
//     use iota_types::base_types::IotaAddress;
//     use tempfile::tempdir;

//     let mut profiler = IngestionLatencyProfiler::new("File System");
//     let num_samples = 10u64; // Reduced for file system test since it's more
// complex     println!(
//         "[File] Starting REAL-TIME latency profiling test with {} samples",
//         num_samples
//     );

//     // Create temporary directory for checkpoint files
//     let temp_dir = tempdir().unwrap();
//     let checkpoint_dir = temp_dir.path().to_path_buf();

//     // Start a test cluster with gRPC enabled (for checkpoint export)
//     let grpc_port = 50055u16;
//     let grpc_addr = format!("127.0.0.1:{}", grpc_port);
//     let cluster = TestClusterBuilder::new()
//         .with_fullnode_grpc_api_address(grpc_addr.clone())
//         .with_num_validators(1)
//         .build()
//         .await;

//     println!("[File] Cluster started, creating initial checkpoint files...");

//     // Set up REST client for fetching checkpoint data
//     let rest_client = iota_rest_api::Client::new(cluster.rpc_url());

//     // Wait for the cluster to have initial checkpoints available
//     cluster.wait_for_checkpoint(2, None).await;

//     // Create checkpoint files for the initial available checkpoints only
//     for seq in 0..=2 {
//         // Fetch and create the checkpoint file
//         let checkpoint_data = rest_client
//             .get_full_checkpoint(seq)
//             .await
//             .expect("Failed to fetch checkpoint");

//         let blob =
//             Blob::encode(&checkpoint_data, BlobEncoding::Bcs).expect("encode
// checkpoint as blob");

//         // Create the checkpoint file
//         let file_path = checkpoint_dir.join(format!("{}.chk", seq));
//         std::fs::write(file_path, blob.to_bytes()).expect("write checkpoint
// file");

//         println!("[File] Created checkpoint file {}.chk", seq);
//     }

//     println!(
//         "[File] Created 3 initial checkpoint files (0-2), starting indexer
// with optimized settings..."     );

//     // Start the indexer configured to process files from checkpoint 0 with
// FAST     // tick interval
//     let db_url =
// "postgres://postgres:postgrespw@localhost:5432/
// iota_indexer_latency_file_realtime"         .to_string();

//     // Use fast indexer configuration for accurate latency measurement
//     let indexer_config = IndexerTypeConfig::writer_mode(None, None);

//     let (store, handle) = start_test_indexer(
//         db_url.clone(),
//         true,
//         None,
//         cluster.rpc_url().to_string(),
//         indexer_config,
//         Some(checkpoint_dir.clone()),
//     )
//     .await;

//     println!("[File] Indexer started, waiting for initial processing...");

//     // Wait for indexer to process the initial files and catch up to current
// state     tokio::time::timeout(std::time::Duration::from_secs(10), async {
//         loop {
//             if let Ok(latest_seq) =
// store.get_latest_checkpoint_sequence_number().await {                 if let
// Some(seq) = latest_seq {                     if seq >= 2 {
//                         println!(
//                             "[File] Indexer processed initial files up to
// checkpoint {}",                             seq
//                         );
//                         break;
//                     }
//                 }
//             }
//             tokio::time::sleep(std::time::Duration::from_millis(200)).await;
//         }
//     })
//     .await
//     .expect("Indexer did not process initial files within timeout");

//     // Now create new transactions and measure latency from file creation to
// indexer     // processing
//     let mut current_checkpoint = 3u64; // Start from the next expected
// checkpoint

//     for i in 1..=num_samples {
//         println!("[File] Creating new transaction {} of {}", i, num_samples);

//         // Create a new transaction to generate a new checkpoint
//         let recipient = IotaAddress::random_for_testing_only();
//         let tx = make_transfer_iota_transaction(&cluster.wallet,
// Some(recipient), None).await;         let _response =
// cluster.execute_transaction(tx).await;

//         // Wait for the new checkpoint to be available
//         cluster.wait_for_checkpoint(current_checkpoint, None).await;

//         // Record when we create the file (checkpoint available for
// processing)         profiler.record_checkpoint_created(current_checkpoint);

//         // Fetch and create the new checkpoint file
//         let checkpoint_data = rest_client
//             .get_full_checkpoint(current_checkpoint)
//             .await
//             .expect("Failed to fetch checkpoint");

//         let blob =
//             Blob::encode(&checkpoint_data, BlobEncoding::Bcs).expect("encode
// checkpoint as blob");

//         let file_path = checkpoint_dir.join(format!("{}.chk",
// current_checkpoint));         std::fs::write(file_path,
// blob.to_bytes()).expect("write checkpoint file");

//         println!(
//             "[File] Created checkpoint file {}.chk, waiting for indexer
// processing...",             current_checkpoint
//         );

//         // Wait for the indexer to process this file
//         let timeout_duration = std::time::Duration::from_secs(15);
//         tokio::time::timeout(timeout_duration, async {
//             loop {
//                 if let Ok(latest_seq) =
// store.get_latest_checkpoint_sequence_number().await {                     if
// let Some(seq) = latest_seq {                         if seq >=
// current_checkpoint {
// profiler.record_checkpoint_indexed(current_checkpoint);
// println!(                                 "[File] Checkpoint {} now available
// in indexer",                                 current_checkpoint
//                             );
//                             break;
//                         }
//                     }
//                 }
//
// tokio::time::sleep(std::time::Duration::from_millis(1)).await; // Check more
// frequently             }
//         })
//         .await
//         .expect(&format!(
//             "Indexer did not process file {} within timeout",
//             current_checkpoint
//         ));

//         current_checkpoint += 1;

//         // Small delay between transactions
//         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//     }

//     println!(
//         "[File] Completed {} real-time file ingestion measurements",
//         num_samples
//     );

//     // Print detailed latency statistics
//     profiler.print_statistics();

//     // Clean up
//     handle.abort();
//     let _ = handle.await;
//     println!("[File] Real-time latency test completed successfully");
// }
