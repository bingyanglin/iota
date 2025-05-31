// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ffi::OsString, fs, path::PathBuf, sync::Arc, time::Duration};

use anyhow;
use backoff::backoff::Backoff;
use iota_grpc_api::{
    conversions::checkpoints::convert_checkpoint_data_gprc_to_core,
    proto::iota::gprc::v1::{
        Direction, GetCheckpointRequest, ListCheckpointsRequest, StreamedCheckpoint,
        SubscribeNewCheckpointsRequest,
        checkpoint_gprc_service_client::CheckpointGprcServiceClient, streamed_checkpoint,
    },
};
use iota_metrics::spawn_monitored_task;
use iota_rest_api::Client as RestClient;
use iota_storage::blob::Blob;
use iota_types::{
    full_checkpoint_content::CheckpointData, messages_checkpoint::CheckpointSequenceNumber,
};
use notify::{RecursiveMode, Watcher};
use object_store::{ObjectStore, path::Path};
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        oneshot,
    },
    time::timeout,
};
use tonic;
use tracing::{debug, error, info};

use crate::{
    IngestionError, IngestionResult, create_remote_store_client,
    executor::MAX_CHECKPOINTS_IN_PROGRESS,
};

type CheckpointResult = IngestionResult<(Arc<CheckpointData>, usize)>;

/// Implements a checkpoint reader that monitors a local directory.
/// Designed for setups where the indexer daemon is colocated with FN.
/// This implementation is push-based and utilizes the inotify API.
pub struct CheckpointReader {
    path: PathBuf,
    remote_store_url: Option<String>,
    remote_store_options: Vec<(String, String)>,
    pub current_checkpoint_number: CheckpointSequenceNumber,
    last_pruned_watermark: CheckpointSequenceNumber,
    checkpoint_sender: mpsc::Sender<Arc<CheckpointData>>,
    processed_receiver: mpsc::Receiver<CheckpointSequenceNumber>,
    remote_fetcher_receiver: Option<mpsc::Receiver<CheckpointResult>>,
    exit_receiver: oneshot::Receiver<()>,
    options: ReaderOptions,
    data_limiter: DataLimiter,
}

/// Options for configuring how the checkpoint reader fetches new checkpoints.
#[derive(Clone)]
pub struct ReaderOptions {
    /// How often to check for new checkpoints, lower values mean faster
    /// detection but more CPU usage.
    ///
    /// Default: 100ms.
    pub tick_interval_ms: u64,
    /// Network request timeout, it applies to remote store operations.
    ///
    /// Default: 5 seconds.
    pub timeout_secs: u64,
    /// Number of maximum concurrent requests to the remote store. Increase it
    /// for backfills, higher values increase throughput but use more resources.
    ///
    /// Default: 10.
    pub batch_size: usize,
    /// Maximum memory (bytes) for batch checkpoint processing to prevent OOM
    /// errors. Zero indicates no limit.
    ///
    /// Default: 0.
    pub data_limit: usize,
    /// Whether to use gRPC streaming for fetching checkpoints if gRPC is
    /// configured. Default: false (uses unary GetCheckpointFull calls).
    pub use_grpc_streaming: bool,
    /// The address of the gRPC server, if gRPC is used.
    pub grpc_address: Option<String>,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            timeout_secs: 5,
            batch_size: 10,
            data_limit: 0,
            use_grpc_streaming: false,
            grpc_address: None,
        }
    }
}

// Define GrpcStreamingReader struct before RemoteStore enum
#[derive(Debug)]
pub struct GrpcStreamingReader {
    grpc_client: CheckpointGprcServiceClient<tonic::transport::Channel>,
    grpc_stream: Option<tonic::Streaming<StreamedCheckpoint>>,
    server_address: String,
}

enum RemoteStore {
    ObjectStore(Box<dyn ObjectStore>),
    Rest(RestClient),
    Grpc(CheckpointGprcServiceClient<tonic::transport::Channel>),
    GrpcStreaming(GrpcStreamingReader),
    Hybrid(Box<dyn ObjectStore>, RestClient),
}

impl CheckpointReader {
    /// Represents a single iteration of the reader.
    /// Reads files in a local directory, validates them, and forwards
    /// `CheckpointData` to the executor.
    async fn read_local_files(&self) -> IngestionResult<Vec<Arc<CheckpointData>>> {
        let mut files = vec![];
        for entry in fs::read_dir(self.path.clone())? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number >= self.current_checkpoint_number {
                    files.push((sequence_number, entry.path()));
                }
            }
        }
        files.sort();
        debug!("unprocessed local files {:?}", files);
        let mut checkpoints = vec![];
        for (_, filename) in files.iter().take(MAX_CHECKPOINTS_IN_PROGRESS) {
            let checkpoint = Blob::from_bytes::<Arc<CheckpointData>>(&fs::read(filename)?)
                .map_err(|err| IngestionError::DeserializeCheckpoint(err.to_string()))?;
            if self.exceeds_capacity(checkpoint.checkpoint_summary.sequence_number) {
                break;
            }
            info!(
                "CheckpointReader::local_fetch: Read checkpoint {} from local filesystem.",
                checkpoint.checkpoint_summary.sequence_number
            );
            checkpoints.push(checkpoint);
        }
        Ok(checkpoints)
    }

    fn exceeds_capacity(&self, checkpoint_number: CheckpointSequenceNumber) -> bool {
        ((MAX_CHECKPOINTS_IN_PROGRESS as u64 + self.last_pruned_watermark) <= checkpoint_number)
            || self.data_limiter.exceeds()
    }

    async fn fetch_from_object_store(
        store: &dyn ObjectStore,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let path = Path::from(format!("{}.chk", checkpoint_number));
        let response = store.get(&path).await?;
        let bytes = response.bytes().await?;
        Ok((
            Blob::from_bytes::<Arc<CheckpointData>>(&bytes)
                .map_err(|err| IngestionError::DeserializeCheckpoint(err.to_string()))?,
            bytes.len(),
        ))
    }

    async fn fetch_from_full_node(
        client: &RestClient,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let checkpoint = client.get_full_checkpoint(checkpoint_number).await?;
        let size = bcs::serialized_size(&checkpoint)?;
        Ok((Arc::new(checkpoint), size))
    }

    async fn fetch_from_full_node_grpc(
        client: &mut CheckpointGprcServiceClient<tonic::transport::Channel>,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let request = tonic::Request::new(GetCheckpointRequest {
            checkpoint_id: checkpoint_number.to_string(),
        });
        let response = client.get_checkpoint_full(request).await?.into_inner();
        let core_checkpoint_data = convert_checkpoint_data_gprc_to_core(response).map_err(|e| {
            IngestionError::Upstream(anyhow::anyhow!("gRPC conversion error: {}", e))
        })?;
        let size = bcs::serialized_size(&core_checkpoint_data)?;
        Ok((Arc::new(core_checkpoint_data), size))
    }

    async fn remote_fetch_checkpoint_internal(
        store: &mut RemoteStore,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        match store {
            RemoteStore::ObjectStore(store) => {
                Self::fetch_from_object_store(store, checkpoint_number).await
            }
            RemoteStore::Rest(client) => {
                Self::fetch_from_full_node(client, checkpoint_number).await
            }
            RemoteStore::Grpc(client) => {
                Self::fetch_from_full_node_grpc(client, checkpoint_number).await
            }
            RemoteStore::Hybrid(store, client) => {
                match Self::fetch_from_full_node(client, checkpoint_number).await {
                    Ok(result) => Ok(result),
                    Err(_) => Self::fetch_from_object_store(store, checkpoint_number).await,
                }
            }
            RemoteStore::GrpcStreaming(reader) => {
                match reader
                    .fetch_from_full_node_grpc_streaming(Some(checkpoint_number))
                    .await
                {
                    Ok(Some((checkpoint_arc, size))) => {
                        info!(
                            "CheckpointReader::remote_fetch (via remote_fetch_checkpoint_internal): Fetched checkpoint {} from gRPC streaming.",
                            checkpoint_arc.checkpoint_summary.sequence_number
                        );
                        Ok((checkpoint_arc, size))
                    }
                    Ok(None) => Err(IngestionError::GrpcCheckpointNotFound(format!(
                        "Stream yielded no checkpoint for requested sequence {}",
                        checkpoint_number
                    ))),
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn remote_fetch_checkpoint(
        store: &mut RemoteStore,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> IngestionResult<(Arc<CheckpointData>, usize)> {
        let mut backoff = backoff::ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));
        backoff.initial_interval = Duration::from_millis(100);
        backoff.current_interval = backoff.initial_interval;
        backoff.multiplier = 1.0;
        loop {
            match Self::remote_fetch_checkpoint_internal(store, checkpoint_number).await {
                Ok(data) => return Ok(data),
                Err(err) => match backoff.next_backoff() {
                    Some(duration) => {
                        if !err.to_string().contains("404") {
                            debug!(
                                "remote reader retry in {} ms. Error is {:?}",
                                duration.as_millis(),
                                err
                            );
                        }
                        tokio::time::sleep(duration).await
                    }
                    None => return Err(err),
                },
            }
        }
    }

    async fn start_remote_fetcher(
        &mut self,
    ) -> IngestionResult<mpsc::Receiver<IngestionResult<(Arc<CheckpointData>, usize)>>> {
        let batch_size = self.options.batch_size;
        let start_checkpoint_from_config = self.current_checkpoint_number;
        let (sender, receiver) = mpsc::channel(batch_size);

        let use_grpc_streaming = self.options.use_grpc_streaming;
        let grpc_address_override = self.options.grpc_address.clone();

        let mut store_for_task = if use_grpc_streaming && grpc_address_override.is_some() {
            let grpc_socket_addr_str = grpc_address_override.unwrap();
            let grpc_uri = format!("http://{}", grpc_socket_addr_str);
            match CheckpointGprcServiceClient::connect(grpc_uri.clone()).await {
                Ok(grpc_client) => {
                    info!(
                        "CheckpointReader: Initializing RemoteStore::GrpcStreaming for URL: {}",
                        grpc_uri
                    );
                    RemoteStore::GrpcStreaming(GrpcStreamingReader {
                        grpc_client,
                        grpc_stream: None, // Stream will be initialized on first fetch
                        server_address: grpc_socket_addr_str, /* Keep original SocketAddr string for
                                            * GrpcStreamingReader */
                    })
                }
                Err(e) => {
                    error!(
                        "Failed to connect to gRPC endpoint {} for streaming: {}. Falling back or erroring.",
                        grpc_uri, e
                    );
                    return Err(IngestionError::Upstream(anyhow::anyhow!(
                        "Failed to connect to gRPC endpoint {} for streaming: {}",
                        grpc_uri,
                        e
                    )));
                }
            }
        } else {
            // Fallback to existing logic if gRPC streaming is not used or no specific gRPC
            // address is provided
            let url = self.remote_store_url.clone().ok_or_else(|| {
                IngestionError::Upstream(anyhow::anyhow!(
                    "Remote store URL not configured for non-gRPC streaming fallback"
                ))
            })?;

            if let Some((fn_url, remote_url)) = url.split_once('|') {
                let object_store = create_remote_store_client(
                    remote_url.to_string(),
                    self.remote_store_options.clone(),
                    self.options.timeout_secs,
                )?;
                RemoteStore::Hybrid(object_store, RestClient::new(fn_url.to_string()))
            } else if url.starts_with("grpc://") {
                // This branch is now less likely if grpc_address_override was handled above,
                // but kept for robustness or if use_grpc_streaming is false but url is grpc.
                if use_grpc_streaming {
                    // Should ideally use grpc_address_override if available
                    match CheckpointGprcServiceClient::connect(url.clone()).await {
                        Ok(grpc_client) => {
                            info!(
                                "CheckpointReader: Initializing RemoteStore::GrpcStreaming (via fallback URL) for URL: {}",
                                url
                            );
                            RemoteStore::GrpcStreaming(GrpcStreamingReader {
                                grpc_client,
                                grpc_stream: None,
                                server_address: url.clone(),
                            })
                        }
                        Err(e) => {
                            error!(
                                "Failed to connect to gRPC endpoint {} (via fallback URL) for streaming: {}.",
                                url, e
                            );
                            return Err(IngestionError::Upstream(anyhow::anyhow!(
                                "Failed to connect to gRPC endpoint {} (via fallback URL) for streaming: {}",
                                url,
                                e
                            )));
                        }
                    }
                } else {
                    match CheckpointGprcServiceClient::connect(url.clone()).await {
                        Ok(client) => {
                            info!(
                                "CheckpointReader: Initializing RemoteStore::Grpc (unary) for URL: {}",
                                url
                            );
                            RemoteStore::Grpc(client)
                        }
                        Err(e) => {
                            error!(
                                "Failed to connect to gRPC endpoint {} for unary: {}.",
                                url, e
                            );
                            return Err(IngestionError::Upstream(anyhow::anyhow!(
                                "Failed to connect to gRPC endpoint {} for unary: {}",
                                url,
                                e
                            )));
                        }
                    }
                }
            } else if url.ends_with("/api/v1") {
                RemoteStore::Rest(RestClient::new(url.to_string()))
            } else {
                let object_store = create_remote_store_client(
                    url.to_string(),
                    self.remote_store_options.clone(),
                    self.options.timeout_secs,
                )?;
                RemoteStore::ObjectStore(object_store)
            }
        };

        spawn_monitored_task!({
            let mut current_checkpoint_to_fetch = start_checkpoint_from_config;
            async move {
                info!(
                    "Entering polling/streaming mode for remote fetcher. Current start checkpoint: {}. Streaming enabled in options: {}",
                    current_checkpoint_to_fetch, use_grpc_streaming
                );

                loop {
                    let mut sent_any_success_in_batch = false;
                    if batch_size == 0 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        current_checkpoint_to_fetch += 1;
                        continue;
                    }

                    for i in 0..batch_size {
                        let checkpoint_num_to_fetch = current_checkpoint_to_fetch + i as u64;
                        let result = Self::remote_fetch_checkpoint(
                            &mut store_for_task,
                            checkpoint_num_to_fetch,
                        )
                        .await;

                        if let Ok((data, size)) = &result {
                            info!(
                                "[CheckpointReader::start_remote_fetcher] Successfully fetched CP {}, size {}. Sending to internal channel.",
                                data.checkpoint_summary.sequence_number, size
                            );
                        } else if let Err(e) = &result {
                            info!(
                                "[CheckpointReader::start_remote_fetcher] Fetch for CP {} failed: {:?}. Error will be sent to internal channel.",
                                checkpoint_num_to_fetch, e
                            );
                        }

                        let result_is_ok = result.is_ok();
                        if sender.send(result).await.is_err() {
                            info!(
                                "Remote reader checkpoint receiver closed, terminating polling task."
                            );
                            return;
                        }
                        if result_is_ok {
                            info!(
                                "[CheckpointReader::start_remote_fetcher] Successfully sent result for CP {} to internal channel.",
                                checkpoint_num_to_fetch
                            );
                            sent_any_success_in_batch = true;
                        }
                    }

                    if !sent_any_success_in_batch && batch_size > 0 {
                        debug!(
                            "All fetches in batch failed, adding small delay before next batch."
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }

                    current_checkpoint_to_fetch += batch_size as u64;
                }
            }
        });

        Ok(receiver)
    }

    async fn remote_fetch(&mut self) -> IngestionResult<Vec<Arc<CheckpointData>>> {
        let mut checkpoints = vec![];
        if self.remote_fetcher_receiver.is_none() {
            self.remote_fetcher_receiver = Some(self.start_remote_fetcher().await?);
        }
        while !self.exceeds_capacity(self.current_checkpoint_number + checkpoints.len() as u64) {
            match self.remote_fetcher_receiver.as_mut().unwrap().try_recv() {
                Ok(Ok((checkpoint, size))) => {
                    self.data_limiter.add(&checkpoint, size);
                    checkpoints.push(checkpoint);
                }
                Ok(Err(err)) => {
                    error!("remote reader transient error {:?}", err);
                    self.remote_fetcher_receiver = None;
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    error!("remote reader channel disconnect error");
                    self.remote_fetcher_receiver = None;
                    break;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(checkpoints)
    }

    async fn sync(&mut self) -> IngestionResult<()> {
        let backoff = backoff::ExponentialBackoff::default();
        let mut checkpoints = backoff::future::retry(backoff, || async {
            self.read_local_files().await.map_err(|err| {
                info!("transient local read error {:?}", err);
                backoff::Error::transient(err)
            })
        })
        .await?;

        let mut read_source: &str = "local";
        if self.remote_store_url.is_some()
            && (checkpoints.is_empty()
                || checkpoints[0].checkpoint_summary.sequence_number
                    > self.current_checkpoint_number)
        {
            checkpoints = self.remote_fetch().await?;
            read_source = "remote";
        } else {
            self.remote_fetcher_receiver = None;
        }

        info!(
            "Read from {}. Current checkpoint number: {}, pruning watermark: {}, new updates: {:?}",
            read_source,
            self.current_checkpoint_number,
            self.last_pruned_watermark,
            checkpoints.len(),
        );
        for checkpoint in checkpoints {
            if read_source == "local"
                && checkpoint.checkpoint_summary.sequence_number > self.current_checkpoint_number
            {
                // For local reads, if we find a file far ahead, stop and wait for intermediate
                // ones. This might not be strictly necessary if executor
                // handles out-of-order, but good for now.
                break;
            }
            // If the received checkpoint is not what we strictly expected next, log and
            // adjust. For streaming, we expect it to be sequential starting
            // from current_checkpoint_number or current_checkpoint_number + 1
            // depending on how the stream was initiated relative to
            // current_checkpoint_number.
            if checkpoint.checkpoint_summary.sequence_number != self.current_checkpoint_number {
                if checkpoint.checkpoint_summary.sequence_number < self.current_checkpoint_number {
                    info!(
                        "CheckpointReader::sync: Received checkpoint {} which is older than current expected {}. Skipping.",
                        checkpoint.checkpoint_summary.sequence_number,
                        self.current_checkpoint_number
                    );
                    continue; // Skip older checkpoints
                }
                info!(
                    "CheckpointReader::sync: Received checkpoint {} while expecting {}. Adjusting current checkpoint number.",
                    checkpoint.checkpoint_summary.sequence_number, self.current_checkpoint_number
                );
                // We will process this checkpoint, so current_checkpoint_number
                // will become this.seq + 1
            }

            tracing::info!(
                "CheckpointReader::sync: Attempting to send CP {} to executor. Current CPNUM: {}",
                checkpoint.checkpoint_summary.sequence_number,
                self.current_checkpoint_number
            );
            self.checkpoint_sender
                .send(checkpoint.clone())
                .await
                .map_err(|e| {
                    tracing::error!(
                        "CheckpointReader::sync: FAILED to send CP to executor: {:?}",
                        e
                    );
                    IngestionError::Channel(
                        "unable to send checkpoint to executor, receiver half closed".to_owned(),
                    )
                })?;
            tracing::info!(
                "CheckpointReader::sync: Successfully sent CP {} to executor. Updated CPNUM to: {}",
                checkpoint.checkpoint_summary.sequence_number,
                checkpoint.checkpoint_summary.sequence_number + 1
            );
            // Update current_checkpoint_number to be the next one we expect
            self.current_checkpoint_number = checkpoint.checkpoint_summary.sequence_number + 1;
        }
        Ok(())
    }

    /// Cleans the local directory by removing all processed checkpoint files.
    fn gc_processed_files(&mut self, watermark: CheckpointSequenceNumber) -> IngestionResult<()> {
        info!("cleaning processed files, watermark is {}", watermark);
        self.data_limiter.gc(watermark);
        self.last_pruned_watermark = watermark;
        for entry in fs::read_dir(self.path.clone())? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number < watermark {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
    }

    fn checkpoint_number_from_file_path(file_name: &OsString) -> Option<CheckpointSequenceNumber> {
        file_name
            .to_str()
            .and_then(|s| s.rfind('.').map(|pos| &s[..pos]))
            .and_then(|s| s.parse().ok())
    }

    pub fn initialize(
        path: PathBuf,
        starting_checkpoint_number: CheckpointSequenceNumber,
        remote_store_url: Option<String>,
        remote_store_options: Vec<(String, String)>,
        options: ReaderOptions,
    ) -> (
        Self,
        mpsc::Receiver<Arc<CheckpointData>>,
        mpsc::Sender<CheckpointSequenceNumber>,
        oneshot::Sender<()>,
    ) {
        let (checkpoint_sender, checkpoint_recv) = mpsc::channel(MAX_CHECKPOINTS_IN_PROGRESS);
        let (processed_sender, processed_receiver) = mpsc::channel(MAX_CHECKPOINTS_IN_PROGRESS);
        let (exit_sender, exit_receiver) = oneshot::channel();
        let reader = Self {
            path,
            remote_store_url,
            remote_store_options,
            current_checkpoint_number: starting_checkpoint_number,
            last_pruned_watermark: starting_checkpoint_number,
            checkpoint_sender,
            processed_receiver,
            remote_fetcher_receiver: None,
            exit_receiver,
            data_limiter: DataLimiter::new(options.data_limit),
            options,
        };
        (reader, checkpoint_recv, processed_sender, exit_sender)
    }

    pub async fn run(mut self) -> IngestionResult<()> {
        let initial_cp_for_log = self.current_checkpoint_number;
        info!(
            "CheckpointReader::run: Starting main loop. Initial CP for log: {}",
            initial_cp_for_log
        );
        let tick_interval = Duration::from_millis(self.options.tick_interval_ms);
        let (inotify_sender, mut inotify_recv) = mpsc::channel(1);
        std::fs::create_dir_all(self.path.clone()).expect("failed to create a directory");
        let mut watcher = notify::recommended_watcher(move |res| {
            if let Err(err) = res {
                eprintln!("watch error: {:?}", err);
            }
            inotify_sender
                .blocking_send(())
                .expect("Failed to send inotify update");
        })
        .expect("Failed to init inotify");

        watcher
            .watch(&self.path, RecursiveMode::NonRecursive)
            .expect("Inotify watcher failed");
        self.gc_processed_files(self.last_pruned_watermark)
            .expect("Failed to clean the directory");

        tracing::info!(
            "CheckpointReader::run: Starting main loop. Path: {:?}, Initial CP: {}",
            self.path,
            self.current_checkpoint_number
        );
        loop {
            tokio::select! {
                _ = &mut self.exit_receiver => {
                    tracing::info!("CheckpointReader::run: Exit signal received, breaking loop.");
                    break;
                }
                Some(gc_checkpoint_number) = self.processed_receiver.recv() => {
                    tracing::info!("CheckpointReader::run: Received GC for CP: {}", gc_checkpoint_number);
                    self.gc_processed_files(gc_checkpoint_number).expect("Failed to clean the directory");
                }
                Ok(Some(_)) | Err(_) = timeout(tick_interval, inotify_recv.recv())  => {
                    tracing::info!(
                        "CheckpointReader::run: Tick or inotify event. Current CP before sync: {}",
                        self.current_checkpoint_number
                    );
                    match self.sync().await {
                        Ok(_) => tracing::info!(
                            "CheckpointReader::run: sync() completed. Current CP after sync: {}",
                            self.current_checkpoint_number
                        ),
                        Err(e) => tracing::error!("CheckpointReader::run: sync() failed: {:?}", e),
                    }
                }
            }
        }
        tracing::info!("CheckpointReader::run: Exited main loop.");
        Ok(())
    }
}

pub struct DataLimiter {
    limit: usize,
    queue: BTreeMap<CheckpointSequenceNumber, usize>,
    in_progress: usize,
}

impl DataLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            queue: BTreeMap::new(),
            in_progress: 0,
        }
    }

    fn exceeds(&self) -> bool {
        self.limit > 0 && self.in_progress >= self.limit
    }

    fn add(&mut self, checkpoint: &CheckpointData, size: usize) {
        if self.limit == 0 {
            return;
        }
        self.in_progress += size;
        self.queue
            .insert(checkpoint.checkpoint_summary.sequence_number, size);
    }

    fn gc(&mut self, watermark: CheckpointSequenceNumber) {
        while self
            .queue
            .first_key_value()
            .map_or(false, |(seq, _)| *seq < watermark)
        {
            if let Some((_, size)) = self.queue.pop_first() {
                self.in_progress -= size;
            }
        }
    }
}

impl GrpcStreamingReader {
    async fn fetch_from_full_node_grpc_streaming(
        &mut self,
        requested_checkpoint_seq_num: Option<CheckpointSequenceNumber>,
    ) -> Result<Option<(Arc<CheckpointData>, usize)>, IngestionError> {
        info!(
            "GrpcStreamingReader::fetch_from_full_node_grpc_streaming called with requested_checkpoint_seq_num: {:?}",
            requested_checkpoint_seq_num
        );
        // If the stream is not initialized, or if it's ended, re-initialize.
        let stream_needs_reinit = self.grpc_stream.is_none();

        if stream_needs_reinit {
            tracing::info!(
                "CORE_READER_STREAM: No active stream for {}. Attempting to subscribe.",
                self.server_address
            );
            let start_from_seq = requested_checkpoint_seq_num.unwrap_or(0);
            info!(
                "GrpcStreamingReader: Initializing new stream, requesting from sequence: {}",
                start_from_seq
            );
            let request = tonic::Request::new(SubscribeNewCheckpointsRequest {
                start_from_checkpoint_sequence_number: Some(start_from_seq.to_string()),
                include_full_data: true,
            });
            match self
                .grpc_client
                .clone()
                .subscribe_new_checkpoints(request)
                .await
            {
                Ok(response) => {
                    tracing::info!(
                        "CORE_READER_STREAM: Successfully subscribed to new checkpoint stream for {}.",
                        self.server_address
                    );
                    self.grpc_stream = Some(response.into_inner());
                }
                Err(e) => {
                    tracing::error!(
                        "CORE_READER_STREAM: Failed to subscribe to checkpoint stream for {}: {:?}",
                        self.server_address,
                        e
                    );
                    return Err(IngestionError::GrpcConnectionError(format!(
                        "Failed to subscribe to gRPC stream from {}: {}",
                        self.server_address, e
                    )));
                }
            }
        }

        if let Some(stream) = &mut self.grpc_stream {
            tracing::info!(
                "CORE_READER_STREAM: Attempting to get next item from active stream for {}.",
                self.server_address
            );
            match stream.message().await {
                Ok(Some(streamed_checkpoint)) => {
                    let cp_type_str = streamed_checkpoint.checkpoint_type.as_ref().map_or(
                        "None".to_string(),
                        |t| match t {
                            streamed_checkpoint::CheckpointType::FullData(_) => {
                                "FullData".to_string()
                            }
                            streamed_checkpoint::CheckpointType::Summary(_) => {
                                "Summary".to_string()
                            }
                        },
                    );
                    tracing::info!(
                        "CORE_READER_STREAM: Received streamed_checkpoint: type {} from {}.",
                        cp_type_str,
                        self.server_address
                    );
                    match streamed_checkpoint.checkpoint_type {
                        Some(streamed_checkpoint::CheckpointType::FullData(cp_data_gprc)) => {
                            let cp_seq = cp_data_gprc
                                .summary
                                .as_ref()
                                .map_or(u64::MAX, |s| s.sequence_number);
                            tracing::info!(
                                "CORE_READER_STREAM: Received FullData for CP {} from {}.",
                                cp_seq,
                                self.server_address
                            );
                            match convert_checkpoint_data_gprc_to_core(cp_data_gprc) {
                                Ok(core_checkpoint_data) => {
                                    tracing::info!(
                                        "CORE_READER_STREAM: Successfully converted gRPC CP {} to Core CheckpointData from {}.",
                                        cp_seq,
                                        self.server_address
                                    );
                                    let size = bcs::serialized_size(&core_checkpoint_data)
                                        .map_err(|e| IngestionError::Bcs(e))?;
                                    return Ok(Some((Arc::new(core_checkpoint_data), size)));
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "CORE_READER_STREAM: Failed to convert gRPC CP {} to Core CheckpointData from {}: {:?}. Skipping item.",
                                        cp_seq,
                                        self.server_address,
                                        e
                                    );
                                }
                            }
                        }
                        Some(streamed_checkpoint::CheckpointType::Summary(summary_gprc)) => {
                            let cp_seq = summary_gprc.sequence_number;
                            tracing::warn!(
                                "CORE_READER_STREAM: Received SummaryOnly for CP {} from {}, but FullData is required. Skipping.",
                                cp_seq,
                                self.server_address
                            );
                        }
                        None => {
                            tracing::warn!(
                                "CORE_READER_STREAM: Received StreamedCheckpoint with no CheckpointType from {}. Skipping.",
                                self.server_address
                            );
                        }
                    }
                }
                Ok(None) => {
                    tracing::info!(
                        "CORE_READER_STREAM: Stream ended (Ok(None)) for {}. Resetting for re-subscription.",
                        self.server_address
                    );
                    self.grpc_stream = None;
                }
                Err(e) => {
                    tracing::error!(
                        "CORE_READER_STREAM: Error receiving message from stream for {}: {:?}. Resetting stream.",
                        self.server_address,
                        e
                    );
                    self.grpc_stream = None;
                    return Err(IngestionError::GrpcMessageError(e.to_string()));
                }
            }
        }
        tracing::info!(
            "CORE_READER_STREAM: fetch_from_full_node_grpc_streaming for {} returning None for this attempt.",
            self.server_address
        );
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

    use iota_grpc_api::proto::iota::gprc::v1::{
        CheckpointDataGprc, CheckpointDigestGprc, CheckpointPageGprc, CheckpointTransactionGprc,
        GetCheckpointRequest, ListCheckpointsRequest, SignedCheckpointSummaryGprc,
        StreamedCheckpoint, SubscribeNewCheckpointsRequest, VerifiedTransactionGprc,
        checkpoint_gprc_service_server::{CheckpointGprcService, CheckpointGprcServiceServer},
    };
    use iota_types::{
        base_types::{IotaAddress, ObjectID, ObjectRef, SequenceNumber as TxSequenceNumber},
        digests::ObjectDigest,
        transaction::TransactionData,
    };
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Status, transport::Server};

    use super::*;

    fn mock_object_id_reader_test() -> ObjectID {
        ObjectID::from_str("0x0000000000000000000000000000000000000000000000000000000000000002")
            .unwrap()
    }

    fn mock_iota_address_reader_test() -> IotaAddress {
        IotaAddress::from(mock_object_id_reader_test())
    }

    fn mock_object_ref_reader_test() -> ObjectRef {
        (
            mock_object_id_reader_test(),
            TxSequenceNumber::from(0),
            ObjectDigest::random(),
        )
    }

    fn mock_raw_tx_bytes() -> Vec<u8> {
        let sender = mock_iota_address_reader_test();
        let recipient = mock_iota_address_reader_test();
        let object_to_transfer = mock_object_ref_reader_test();
        let gas_payment_object = mock_object_ref_reader_test();
        let gas_budget = 10_000;
        let gas_price = 1;

        let tx_data = TransactionData::new_transfer(
            recipient,
            object_to_transfer,
            sender,
            gas_payment_object,
            gas_budget,
            gas_price,
        );
        bcs::to_bytes(&tx_data).expect("BCS serialization of mock TransactionData failed")
    }

    #[derive(Debug, Default)]
    struct MockCheckpointService {
        mock_response: Arc<Mutex<Option<CheckpointDataGprc>>>,
        expected_sequence_number: Option<u64>,
    }

    impl MockCheckpointService {
        fn new(response: CheckpointDataGprc, expected_seq_num: u64) -> Self {
            Self {
                mock_response: Arc::new(Mutex::new(Some(response))),
                expected_sequence_number: Some(expected_seq_num),
            }
        }
    }

    #[tonic::async_trait]
    impl CheckpointGprcService for MockCheckpointService {
        type SubscribeNewCheckpointsStream =
            tokio_stream::wrappers::ReceiverStream<Result<StreamedCheckpoint, Status>>;

        async fn get_checkpoint_full(
            &self,
            request: tonic::Request<GetCheckpointRequest>,
        ) -> Result<tonic::Response<CheckpointDataGprc>, tonic::Status> {
            let req_inner = request.into_inner();
            println!(
                "MockService: Received GetCheckpointFull for id: {}",
                req_inner.checkpoint_id
            );

            if let Some(expected_seq) = self.expected_sequence_number {
                let requested_seq = req_inner
                    .checkpoint_id
                    .parse::<u64>()
                    .map_err(|_| Status::invalid_argument("Invalid checkpoint_id format"))?;
                if requested_seq != expected_seq {
                    return Err(Status::invalid_argument(format!(
                        "Expected seq {}, got {}",
                        expected_seq, requested_seq
                    )));
                }
            }

            let mut mock_response_guard = self.mock_response.lock().await;
            if let Some(response) = mock_response_guard.take() {
                Ok(tonic::Response::new(response))
            } else {
                Err(tonic::Status::not_found(
                    "Checkpoint not found or already served",
                ))
            }
        }

        async fn get_checkpoint(
            &self,
            _request: tonic::Request<GetCheckpointRequest>,
        ) -> Result<tonic::Response<SignedCheckpointSummaryGprc>, tonic::Status> {
            println!("MockService: Received GetCheckpoint (summary)");
            Err(tonic::Status::unimplemented(
                "get_checkpoint (summary) not fully implemented in this mock for reader tests",
            ))
        }

        async fn list_checkpoints(
            &self,
            _request: tonic::Request<ListCheckpointsRequest>,
        ) -> Result<tonic::Response<CheckpointPageGprc>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "list_checkpoints not implemented in mock",
            ))
        }

        async fn subscribe_new_checkpoints(
            &self,
            request: tonic::Request<SubscribeNewCheckpointsRequest>,
        ) -> Result<tonic::Response<Self::SubscribeNewCheckpointsStream>, Status> {
            println!(
                "MockService: Received SubscribeNewCheckpoints: {:?}",
                request.get_ref()
            );
            let req_inner = request.into_inner();
            let start_from_seq = req_inner
                .start_from_checkpoint_sequence_number
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let (tx, rx) = mpsc::channel(10);
            let num_checkpoints_to_stream = 3;

            let _mock_response_template = self.mock_response.clone();

            tokio::spawn(async move {
                for i in 0..num_checkpoints_to_stream {
                    let current_seq = start_from_seq + i;
                    let gprc_data = mock_checkpoint_data_gprc(current_seq);

                    let streamed_item = StreamedCheckpoint {
                        checkpoint_type: Some(streamed_checkpoint::CheckpointType::FullData(
                            gprc_data,
                        )),
                    };
                    if tx.send(Ok(streamed_item)).await.is_err() {
                        println!("MockService: Stream receiver dropped.");
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                println!("MockService: Finished streaming mock checkpoints.");
            });

            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    fn mock_checkpoint_data_gprc(sequence_number: u64) -> CheckpointDataGprc {
        CheckpointDataGprc {
            summary: Some(SignedCheckpointSummaryGprc {
                epoch: 0,
                sequence_number,
                network_total_transactions: 100 + sequence_number,
                content_digest: Some(CheckpointDigestGprc { digest: vec![1; 32] }),
                previous_digest: if sequence_number > 0 {
                    Some(CheckpointDigestGprc { digest: vec![2; 32] })
                } else {
                    None
                },
            }),
            transactions: vec![CheckpointTransactionGprc {
                content: Some(
                    iota_grpc_api::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::FullTransaction(
                        VerifiedTransactionGprc { raw_tx: mock_raw_tx_bytes() },
                    ),
                ),
            }],
        }
    }

    async fn start_mock_server(
        service: MockCheckpointService,
    ) -> Result<SocketAddr, anyhow::Error> {
        let initial_addr: SocketAddr = "127.0.0.1:0".parse()?;
        let listener = tokio::net::TcpListener::bind(initial_addr).await?;
        let actual_addr = listener.local_addr()?;

        let server_builder =
            Server::builder().add_service(CheckpointGprcServiceServer::new(service));

        tokio::spawn(async move {
            if let Err(e) = server_builder
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
            {
                eprintln!("Mock server error: {:?}", e);
            }
        });

        Ok(actual_addr)
    }

    #[tokio::test]
    async fn test_remote_fetch_checkpoint_grpc_success() {
        let seq_num = 5u64;
        let mock_gprc_data = mock_checkpoint_data_gprc(seq_num);
        let mock_service = MockCheckpointService::new(mock_gprc_data.clone(), seq_num);
        let server_addr = start_mock_server(mock_service)
            .await
            .expect("Mock server failed to start");
        let grpc_url = format!("grpc://{}", server_addr);
        let mut remote_store = match CheckpointGprcServiceClient::connect(grpc_url.clone()).await {
            Ok(client) => RemoteStore::Grpc(client),
            Err(e) => panic!("Failed to connect to test gRPC server {}: {}", grpc_url, e),
        };
        let (checkpoint_data_arc, _size) =
            CheckpointReader::remote_fetch_checkpoint_internal(&mut remote_store, seq_num)
                .await
                .unwrap();
        assert_eq!(
            checkpoint_data_arc.checkpoint_summary.sequence_number,
            seq_num
        );
        let core_expected_data: iota_types::full_checkpoint_content::CheckpointData =
            convert_checkpoint_data_gprc_to_core(mock_gprc_data).unwrap();
        assert_eq!(
            checkpoint_data_arc.checkpoint_summary.data(),
            core_expected_data.checkpoint_summary.data()
        );
    }

    #[tokio::test]
    async fn test_grpc_streaming_fetch() -> Result<(), Box<dyn std::error::Error>> {
        let start_seq = 10u64;
        let mock_service = MockCheckpointService::default();
        let server_addr = start_mock_server(mock_service).await?;
        let grpc_url = format!("grpc://{}", server_addr);

        let temp_dir = tempfile::tempdir()?;
        let (mut reader, _checkpoint_receiver, _processed_sender, _exit_sender) =
            CheckpointReader::initialize(
                temp_dir.path().to_path_buf(),
                start_seq,
                Some(grpc_url.clone()),
                vec![],
                ReaderOptions {
                    use_grpc_streaming: true,
                    batch_size: 1,
                    ..Default::default()
                },
            );

        let mut actual_stream_receiver = reader.start_remote_fetcher().await.unwrap();
        let mut received_count = 0;
        let expected_stream_count = 3; // Mock server sends 3 per subscription

        for i in 0..expected_stream_count {
            match timeout(Duration::from_secs(2), actual_stream_receiver.recv()).await {
                Ok(Some(Ok((checkpoint_data, _size)))) => {
                    assert_eq!(
                        checkpoint_data.checkpoint_summary.sequence_number,
                        start_seq + i as u64
                    );
                    received_count += 1;
                }
                Ok(Some(Err(e))) => {
                    panic!("Stream received an error during expected items: {:?}", e)
                }
                Ok(None) => panic!("Stream closed (Ok(None)) earlier than expected."),
                Err(_) => panic!("Timeout waiting for checkpoint from stream"),
            }
        }
        assert_eq!(received_count, expected_stream_count);

        // The CheckpointReader is designed to be a continuous source.
        // If the underlying gRPC stream ends and the reader is asked for more data,
        // it will attempt to re-establish the stream.
        // Therefore, asserting that the stream is permanently exhausted after N items
        // is contrary to its design. This test now only verifies the reception
        // of the initial set of streamed items.

        Ok(())
    }
}
