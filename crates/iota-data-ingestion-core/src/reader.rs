// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ffi::OsString, fs, path::PathBuf, sync::Arc, time::Duration};

use anyhow;
use backoff::backoff::Backoff;
use futures::StreamExt;
use iota_gprc_api::{
    conversions::checkpoints::convert_checkpoint_data_gprc_to_core,
    proto::iota::gprc::v1::{
        GetCheckpointRequest, SubscribeNewCheckpointsRequest,
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
use tracing::{debug, error, info, warn};

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
    current_checkpoint_number: CheckpointSequenceNumber,
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
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            timeout_secs: 5,
            batch_size: 10,
            data_limit: 0,
            use_grpc_streaming: false,
        }
    }
}

enum RemoteStore {
    ObjectStore(Box<dyn ObjectStore>),
    Rest(RestClient),
    Grpc(CheckpointGprcServiceClient<tonic::transport::Channel>),
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
        let url = self.remote_store_url.clone().ok_or_else(|| {
            IngestionError::Upstream(anyhow::anyhow!("Remote store URL not configured"))
        })?;

        let use_grpc_streaming = self.options.use_grpc_streaming;
        let reader_options_clone = self.options.clone();

        let mut store_for_task = if let Some((fn_url, remote_url)) = url.split_once('|') {
            let object_store = create_remote_store_client(
                remote_url.to_string(),
                self.remote_store_options.clone(),
                self.options.timeout_secs,
            )?;
            RemoteStore::Hybrid(object_store, RestClient::new(fn_url.to_string()))
        } else if url.starts_with("grpc://") {
            match CheckpointGprcServiceClient::connect(url.clone()).await {
                Ok(client) => RemoteStore::Grpc(client),
                Err(e) => {
                    return Err(IngestionError::Upstream(anyhow::anyhow!(
                        "Failed to connect to gRPC endpoint {}: {}",
                        url,
                        e
                    )));
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
        };

        spawn_monitored_task!({
            let mut current_checkpoint_to_fetch = start_checkpoint_from_config;
            async move {
                if use_grpc_streaming && matches!(store_for_task, RemoteStore::Grpc(_)) {
                    if let RemoteStore::Grpc(mut client) = store_for_task {
                        info!(
                            "Starting gRPC streaming from checkpoint: {}",
                            current_checkpoint_to_fetch
                        );
                        let request = tonic::Request::new(SubscribeNewCheckpointsRequest {
                            start_from_sequence_number: Some(
                                current_checkpoint_to_fetch.to_string(),
                            ),
                            include_full_data: true,
                        });

                        match client.subscribe_new_checkpoints(request).await {
                            Ok(response) => {
                                let mut stream = response.into_inner();
                                while let Some(item_result) = stream.next().await {
                                    match item_result {
                                        Ok(streamed_checkpoint) => {
                                            if let Some(checkpoint_type) =
                                                streamed_checkpoint.checkpoint_type
                                            {
                                                match checkpoint_type {
                                                    streamed_checkpoint::CheckpointType::FullData(gprc_data) => {
                                                        let seq_num = gprc_data.summary.as_ref().map_or(0, |s| s.sequence_number);
                                                        match convert_checkpoint_data_gprc_to_core(gprc_data) {
                                                            Ok(core_data) => {
                                                                let size = bcs::serialized_size(&core_data).unwrap_or(0);
                                                                if sender.send(Ok((Arc::new(core_data), size))).await.is_err() {
                                                                    error!("Checkpoint receiver closed, terminating gRPC stream.");
                                                                    break;
                                                                }
                                                                current_checkpoint_to_fetch = seq_num + 1;
                                                            }
                                                            Err(e) => {
                                                                error!("gRPC stream: Failed to convert checkpoint {}: {:?}", seq_num, e);
                                                                if sender.send(Err(IngestionError::Upstream(anyhow::anyhow!("gRPC conversion error: {}", e)))).await.is_err() {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    streamed_checkpoint::CheckpointType::Summary(summary_data) => {
                                                        info!(
                                                            "gRPC stream: Received summary for checkpoint {}, skipping as full data is expected.",
                                                            summary_data.sequence_number
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        Err(status) => {
                                            error!(
                                                "gRPC stream error: {:?}. Attempting to re-establish.",
                                                status
                                            );
                                            if sender
                                                .send(Err(IngestionError::Upstream(
                                                    anyhow::anyhow!(
                                                        "gRPC stream error: {}",
                                                        status
                                                    ),
                                                )))
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                            break;
                                        }
                                    }
                                }
                                info!(
                                    "gRPC checkpoint stream ended. Next expected checkpoint if resumed: {}",
                                    current_checkpoint_to_fetch
                                );
                            }
                            Err(status) => {
                                error!(
                                    "Failed to subscribe to gRPC checkpoint stream: {:?}",
                                    status
                                );
                                let _ = sender
                                    .send(Err(IngestionError::Upstream(anyhow::anyhow!(
                                        "Failed to subscribe to gRPC stream: {}",
                                        status
                                    ))))
                                    .await;
                            }
                        }
                        return;
                    } else {
                        warn!("gRPC streaming mode with non-Grpc store, this is unexpected.");
                    }
                }

                info!(
                    "Entering polling mode for remote fetcher. Current start checkpoint: {}. Streaming: {}",
                    current_checkpoint_to_fetch, use_grpc_streaming
                );

                loop {
                    let mut sent_any_success_in_batch = false;
                    if reader_options_clone.batch_size == 0 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        current_checkpoint_to_fetch += 1;
                        continue;
                    }

                    for i in 0..reader_options_clone.batch_size {
                        let checkpoint_num_to_fetch = current_checkpoint_to_fetch + i as u64;
                        let result = Self::remote_fetch_checkpoint(
                            &mut store_for_task,
                            checkpoint_num_to_fetch,
                        )
                        .await;

                        let result_is_ok = result.is_ok();
                        if sender.send(result).await.is_err() {
                            info!(
                                "Remote reader checkpoint receiver closed, terminating polling task."
                            );
                            return;
                        }
                        if result_is_ok {
                            sent_any_success_in_batch = true;
                        }
                    }

                    if !sent_any_success_in_batch && reader_options_clone.batch_size > 0 {
                        debug!(
                            "All fetches in batch failed, adding small delay before next batch."
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }

                    current_checkpoint_to_fetch += reader_options_clone.batch_size as u64;
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
                break;
            }
            assert_eq!(
                checkpoint.checkpoint_summary.sequence_number,
                self.current_checkpoint_number
            );
            self.checkpoint_sender.send(checkpoint).await.map_err(|_| {
                IngestionError::Channel(
                    "unable to send checkpoint to executor, receiver half closed".to_owned(),
                )
            })?;
            self.current_checkpoint_number += 1;
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

        loop {
            tokio::select! {
                _ = &mut self.exit_receiver => break,
                Some(gc_checkpoint_number) = self.processed_receiver.recv() => {
                    self.gc_processed_files(gc_checkpoint_number).expect("Failed to clean the directory");
                }
                Ok(Some(_)) | Err(_) = timeout(Duration::from_millis(self.options.tick_interval_ms), inotify_recv.recv())  => {
                    self.sync().await.expect("Failed to read checkpoint files");
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

    use iota_gprc_api::proto::iota::gprc::v1::{
        CheckpointDataGprc, CheckpointDigestGprc, CheckpointPageGprc, CheckpointTransactionGprc,
        GetCheckpointRequest, ListCheckpointsRequest, SignedCheckpointSummaryGprc,
        StreamCheckpointsInRangeRequest, StreamedCheckpoint, SubscribeNewCheckpointsRequest,
        VerifiedTransactionGprc,
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
        type StreamCheckpointsInRangeStream =
            tokio_stream::wrappers::ReceiverStream<Result<StreamedCheckpoint, Status>>;
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
                // Take the response to simulate it being consumed
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
            // For this test suite, we are focused on GetCheckpoint logic becomes relevant
            // for the reader.
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

        async fn stream_checkpoints_in_range(
            &self,
            _request: tonic::Request<StreamCheckpointsInRangeRequest>,
        ) -> Result<tonic::Response<Self::StreamCheckpointsInRangeStream>, Status> {
            Err(tonic::Status::unimplemented(
                "stream_checkpoints_in_range not implemented in mock",
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
                .start_from_sequence_number
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            // For the test, let's stream a few checkpoints starting from `start_from_seq`
            let (tx, rx) = mpsc::channel(10); // Buffer size for the stream
            let num_checkpoints_to_stream = 3;

            let _mock_response_template = self.mock_response.clone(); // Prefix with _

            tokio::spawn(async move {
                for i in 0..num_checkpoints_to_stream {
                    let current_seq = start_from_seq + i;
                    // Create a mock StreamedCheckpoint with FullData
                    // Use the mock_response logic if available, or create fresh mock data.
                    // For simplicity, creating fresh mock_checkpoint_data_gprc here.
                    let gprc_data = mock_checkpoint_data_gprc(current_seq);

                    let streamed_item = StreamedCheckpoint {
                        checkpoint_type: Some(streamed_checkpoint::CheckpointType::FullData(
                            gprc_data,
                        )),
                    };
                    if tx.send(Ok(streamed_item)).await.is_err() {
                        // Receiver dropped, stop sending
                        println!("MockService: Stream receiver dropped.");
                        break;
                    }
                    // Simulate some delay between streaming items
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                println!("MockService: Finished streaming mock checkpoints.");
                // Dropping tx will close the stream on the client side
            });

            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    // Helper to create a mock CheckpointDataGprc
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
                    iota_gprc_api::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::FullTransaction(
                        VerifiedTransactionGprc { raw_tx: mock_raw_tx_bytes() },
                    ),
                ),
            }],
        }
    }

    // Helper to start the mock server
    async fn start_mock_server(
        service: MockCheckpointService,
    ) -> Result<SocketAddr, anyhow::Error> {
        let initial_addr: SocketAddr = "127.0.0.1:0".parse()?; // For TcpListener
        let listener = tokio::net::TcpListener::bind(initial_addr).await?;
        let actual_addr = listener.local_addr()?; // Get the OS-assigned port

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

        Ok(actual_addr) // Return the actual address
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
        // The mock_response in MockCheckpointService isn't directly used by
        // subscribe_new_checkpoints, as it generates its own stream. So, the
        // specific data doesn't matter for the constructor here.
        let mock_service = MockCheckpointService::default(); // Or new with any dummy data
        let server_addr = start_mock_server(mock_service).await?;
        let grpc_url = format!("grpc://{}", server_addr);

        let temp_dir = tempfile::tempdir()?;
        let (mut reader, _checkpoint_receiver, _processed_sender, _exit_sender) =
            CheckpointReader::initialize(
                temp_dir.path().to_path_buf(),
                start_seq, // Start current_checkpoint_number from here
                Some(grpc_url.clone()),
                vec![],
                ReaderOptions {
                    use_grpc_streaming: true,
                    batch_size: 1, // Keep low for predictable stream item count
                    ..Default::default()
                },
            );

        let mut actual_stream_receiver = reader.start_remote_fetcher().await.unwrap();
        let mut received_count = 0;
        let expected_stream_count = 3; // Mock service streams 3 items

        for i in 0..expected_stream_count {
            match timeout(Duration::from_secs(2), actual_stream_receiver.recv()).await {
                Ok(Some(Ok((checkpoint_data, _size)))) => {
                    assert_eq!(
                        checkpoint_data.checkpoint_summary.sequence_number,
                        start_seq + i as u64
                    );
                    received_count += 1;
                }
                Ok(Some(Err(e))) => panic!("Stream received an error: {:?}", e),
                Ok(None) => panic!("Stream closed earlier than expected."),
                Err(_) => panic!("Timeout waiting for checkpoint from stream"),
            }
        }
        assert_eq!(received_count, expected_stream_count);

        // Check that the stream can close gracefully
        match timeout(Duration::from_millis(200), actual_stream_receiver.recv()).await {
            Ok(None) => { /* Expected: stream closed after mock server finishes */ }
            Ok(Some(_)) => panic!("Stream sent more items than expected."),
            Err(_) => {
                // Timeout is also acceptable if stream is just idle
                info!("Stream idle as expected after items.");
            }
        }

        Ok(())
    }
}
