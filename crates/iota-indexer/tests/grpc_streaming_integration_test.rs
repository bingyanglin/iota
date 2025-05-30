// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, /* BTreeSet, */ HashMap},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use diesel::prelude::*;
use iota_grpc_api::proto::iota::gprc::v1::{
    CheckpointDataGprc, CheckpointDigestGprc, CheckpointTransactionGprc, GetCheckpointRequest,
    ListCheckpointsRequest, SignedCheckpointSummaryGprc, StreamedCheckpoint,
    SubscribeNewCheckpointsRequest, VerifiedTransactionGprc,
    checkpoint_gprc_service_server::{CheckpointGprcService, CheckpointGprcServiceServer},
    streamed_checkpoint,
};
use iota_indexer::{
    models::checkpoints::StoredCheckpoint,
    schema::checkpoints::dsl::*,
    test_utils::{IndexerTypeConfig, SnapshotLagConfig, start_test_indexer_impl},
};
use iota_types::{
    base_types::{
        AuthorityName, // EpochId,
        IotaAddress, ObjectDigest, ObjectID, ObjectRef, SequenceNumber as TxSequenceNumber,
    },
    crypto::{
        AuthorityKeyPair, AuthoritySignInfo, // Ed25519IotaSignature,
        // IotaSignatureInner,
        KeypairTraits, // ToFromBytes,
    },
    digests::{CheckpointContentsDigest, CheckpointDigest},
    gas::GasCostSummary,
    message_envelope::{Envelope, Message, VerifiedEnvelope},
    messages_checkpoint::{
        CheckpointContents, CheckpointSequenceNumber, CheckpointSummary, VerifiedCheckpoint,
    },
    transaction::{/* SenderSignedData, Transaction, */ TransactionData},
};
use shared_crypto::intent::Intent;
use tokio::{
    sync::{Mutex, Notify, broadcast, mpsc},
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use tracing_subscriber;

mod common;

// --- Mock gRPC Service Implementation and Helpers ---
fn mock_object_id_indexer_test() -> ObjectID {
    "0x0000000000000000000000000000000000000000000000000000000000000042"
        .parse()
        .unwrap()
}

fn mock_iota_address_indexer_test() -> IotaAddress {
    IotaAddress::from(mock_object_id_indexer_test())
}

fn mock_object_ref_indexer_test() -> ObjectRef {
    (
        mock_object_id_indexer_test(),
        TxSequenceNumber::from(0),
        ObjectDigest::random(),
    )
}

fn mock_raw_tx_bytes_indexer_test() -> Vec<u8> {
    let sender = mock_iota_address_indexer_test();
    let recipient = mock_iota_address_indexer_test();
    let object_to_transfer = mock_object_ref_indexer_test();
    let gas_payment_object = mock_object_ref_indexer_test();
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

fn create_mock_verified_checkpoint_for_indexer(
    seq_num: CheckpointSequenceNumber,
    epoch_id: u64,
) -> VerifiedCheckpoint {
    let summary = CheckpointSummary {
        epoch: epoch_id,
        sequence_number: seq_num,
        network_total_transactions: 1000 + seq_num,
        content_digest: CheckpointContentsDigest::new([0u8; 32]),
        previous_digest: if seq_num > 0 {
            Some(CheckpointDigest::new([1u8; 32]))
        } else {
            None
        },
        epoch_rolling_gas_cost_summary: GasCostSummary::default(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64,
        checkpoint_commitments: vec![],
        end_of_epoch_data: None,
        version_specific_data: vec![],
    };
    let keypair: AuthorityKeyPair = AuthorityKeyPair::generate(&mut rand::thread_rng());
    let authority_name = AuthorityName::from(keypair.public());
    let committee =
        iota_types::committee::Committee::new(epoch_id, BTreeMap::from([(authority_name, 10000)]));
    let sign_info = AuthoritySignInfo::new(
        epoch_id,
        &summary,
        Intent::iota_app(CheckpointSummary::SCOPE),
        authority_name,
        &keypair,
    );
    let sig = iota_types::crypto::AuthorityStrongQuorumSignInfo::new_from_auth_sign_infos(
        vec![sign_info],
        &committee,
    )
    .expect("Mock AuthStrongQuorumSignInfo creation failed");
    VerifiedEnvelope::new_unchecked(Envelope::new_from_data_and_sig(summary, sig))
}

#[derive(Clone)]
struct MockCheckpointService {
    checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
    latest_sequence_number: Arc<AtomicU64>,
    checkpoint_cache: Arc<Mutex<HashMap<CheckpointSequenceNumber, Arc<VerifiedCheckpoint>>>>,
    contents_cache: Arc<Mutex<HashMap<CheckpointSequenceNumber, CheckpointContents>>>,
    subscriber_connected_notify: Arc<Notify>,
}

impl MockCheckpointService {
    fn new(
        checkpoint_event_sender: broadcast::Sender<Arc<VerifiedCheckpoint>>,
        initial_latest_sequence_number: CheckpointSequenceNumber,
        checkpoint_cache: Arc<Mutex<HashMap<CheckpointSequenceNumber, Arc<VerifiedCheckpoint>>>>,
        contents_cache: Arc<Mutex<HashMap<CheckpointSequenceNumber, CheckpointContents>>>,
    ) -> Self {
        Self {
            checkpoint_event_sender,
            latest_sequence_number: Arc::new(AtomicU64::from(initial_latest_sequence_number)),
            checkpoint_cache,
            contents_cache,
            subscriber_connected_notify: Arc::new(Notify::new()),
        }
    }

    async fn _set_latest_sequence_number_for_test(&self, val: u64) {
        let last_known = self.latest_sequence_number.swap(val, Ordering::SeqCst);
        if val > last_known {
            let mut cache_guard = self.checkpoint_cache.lock().await;
            for seq_to_publish in (last_known + 1)..=val {
                let checkpoint = cache_guard
                    .entry(seq_to_publish)
                    .or_insert_with(|| {
                        Arc::new(create_mock_verified_checkpoint_for_indexer(
                            seq_to_publish,
                            1,
                        ))
                    })
                    .clone();

                if self.checkpoint_event_sender.send(checkpoint).is_err() {
                    warn!(
                        "[MockCheckpointService-IndexerTest] Failed to send checkpoint {} event, no active subscribers.",
                        seq_to_publish
                    );
                } else {
                    info!(
                        "[MockCheckpointService-IndexerTest] Successfully sent checkpoint {} event.",
                        seq_to_publish
                    );
                }
            }
        }
    }
}

#[tonic::async_trait]
impl CheckpointGprcService for MockCheckpointService {
    type SubscribeNewCheckpointsStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamedCheckpoint, Status>>;

    async fn get_checkpoint_full(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<CheckpointDataGprc>, Status> {
        let req_id_str = request.into_inner().checkpoint_id;
        info!(
            "[MockCheckpointService-IndexerTest] Received GetCheckpointFull for id: {}",
            req_id_str
        );
        if let Ok(seq_num) = req_id_str.parse::<u64>() {
            let cache_guard = self.checkpoint_cache.lock().await;
            if cache_guard.get(&seq_num).is_some() {
                let gprc_data = mock_checkpoint_data_gprc_indexer_test(seq_num);
                return Ok(Response::new(gprc_data));
            }
        }
        Err(Status::not_found(format!(
            "Checkpoint full data for id '{}' not found in mock cache.",
            req_id_str
        )))
    }

    async fn get_checkpoint(
        &self,
        _request: Request<GetCheckpointRequest>,
    ) -> Result<Response<SignedCheckpointSummaryGprc>, Status> {
        Err(Status::unimplemented(
            "get_checkpoint not implemented in mock for indexer test yet",
        ))
    }

    async fn list_checkpoints(
        &self,
        _request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<iota_grpc_api::proto::iota::gprc::v1::CheckpointPageGprc>, Status> {
        Err(Status::unimplemented(
            "list_checkpoints not implemented in mock for indexer test yet",
        ))
    }

    async fn subscribe_new_checkpoints(
        &self,
        request: Request<SubscribeNewCheckpointsRequest>,
    ) -> Result<Response<Self::SubscribeNewCheckpointsStream>, Status> {
        let inner_request = request.into_inner();
        let start_from_seq_str = inner_request.start_from_checkpoint_sequence_number;
        let include_full_data = inner_request.include_full_data;

        let filter_seq_num = start_from_seq_str
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        info!(
            "[MockCheckpointService-IndexerTest] New subscription. Starting from seq: {}, include_full_data: {}",
            filter_seq_num, include_full_data
        );

        let mut rx = self.checkpoint_event_sender.subscribe();
        let (tx_stream, rx_stream) = mpsc::channel(128);
        let _checkpoint_cache_clone = self.checkpoint_cache.clone();
        let _contents_cache_clone = self.contents_cache.clone();
        let notify_clone = self.subscriber_connected_notify.clone();

        tokio::spawn(async move {
            notify_clone.notify_one();
            info!("[MockSvcSub-{}] Task started.", filter_seq_num);
            loop {
                match rx.recv().await {
                    Ok(verified_checkpoint) => {
                        let checkpoint_seq_num_val = *verified_checkpoint.inner().sequence_number();
                        info!(
                            "[MockSvcSub-{}] Received broadcast CP {}. Filter is >= {}. Condition: {}",
                            filter_seq_num,
                            checkpoint_seq_num_val,
                            filter_seq_num,
                            checkpoint_seq_num_val >= filter_seq_num
                        );
                        if checkpoint_seq_num_val >= filter_seq_num {
                            info!(
                                "[MockSvcSub-{}] Forwarding checkpoint {} to subscriber.",
                                filter_seq_num, checkpoint_seq_num_val
                            );

                            let streamed_item_result: Result<StreamedCheckpoint, Status> =
                                if include_full_data {
                                    let gprc_data = mock_checkpoint_data_gprc_indexer_test(
                                        checkpoint_seq_num_val,
                                    );
                                    Ok(StreamedCheckpoint {
                                        checkpoint_type: Some(
                                            streamed_checkpoint::CheckpointType::FullData(
                                                gprc_data,
                                            ),
                                        ),
                                    })
                                } else {
                                    match iota_grpc_api::conversions::checkpoints::convert_verified_checkpoint_to_gprc_summary(&verified_checkpoint) {
                                    Ok(summary_gprc) => Ok(StreamedCheckpoint {
                                        checkpoint_type: Some(streamed_checkpoint::CheckpointType::Summary(summary_gprc)),
                                    }),
                                    Err(e) => {
                                        error!("[MockCheckpointService-IndexerTest] Error converting checkpoint {} to summary for stream: {:?}", checkpoint_seq_num_val, e);
                                        Err(Status::internal(format!("Error converting checkpoint for stream: {}", e)))
                                    }
                                }
                                };

                            match streamed_item_result {
                                Ok(streamed_item) => {
                                    if tx_stream.send(Ok(streamed_item)).await.is_err() {
                                        info!(
                                            "[MockSvcSub-{}] Client stream disconnected.",
                                            filter_seq_num
                                        );
                                        break;
                                    }
                                }
                                Err(status) => {
                                    if tx_stream.send(Err(status)).await.is_err() {
                                        info!(
                                            "[MockSvcSub-{}] Client stream (post-error) disconnected.",
                                            filter_seq_num
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            "[MockSvcSub-{}] Subscriber lagged by {} messages.",
                            filter_seq_num, n
                        );
                        if tx_stream.send(Err(Status::data_loss(format!(
                            "Lagged by {} messages, subscription starting from {} might have missed data.",
                            n, filter_seq_num
                        )))).await.is_err() {
                             info!(
                                "[MockSvcSub-{}] Client stream (post-lag) disconnected.", filter_seq_num
                            );
                        }
                        info!("[MockSvcSub-{}] Breaking due to Lagged.", filter_seq_num);
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("[MockSvcSub-{}] Broadcast channel closed.", filter_seq_num);
                        break;
                    }
                }
            }
            info!(
                "[MockSvcSub-{}] Terminating forwarder task.",
                filter_seq_num
            );
        });

        Ok(Response::new(ReceiverStream::new(rx_stream)))
    }
}

fn mock_checkpoint_data_gprc_indexer_test(sequence_number_val: u64) -> CheckpointDataGprc {
    let epoch_val = if sequence_number_val == 0 {
        0
    } else {
        sequence_number_val
    };
    let network_total_transactions_val = 100 + sequence_number_val;

    let previous_digest_gprc_opt = if sequence_number_val > 0 {
        Some(CheckpointDigestGprc {
            digest: CheckpointDigest::new([(sequence_number_val - 1) as u8; 32])
                .into_inner()
                .to_vec(),
        })
    } else {
        None
    };

    let mock_content_digest_bytes = CheckpointContentsDigest::new([0u8; 32])
        .into_inner()
        .to_vec();

    CheckpointDataGprc {
        summary: Some(SignedCheckpointSummaryGprc {
            epoch: epoch_val,
            sequence_number: sequence_number_val,
            network_total_transactions: network_total_transactions_val,
            content_digest: Some(CheckpointDigestGprc { digest: mock_content_digest_bytes }),
            previous_digest: previous_digest_gprc_opt,
        }),
        transactions: vec![CheckpointTransactionGprc {
            content: Some(
                iota_grpc_api::proto::iota::gprc::v1::checkpoint_transaction_gprc::Content::FullTransaction(
                    VerifiedTransactionGprc { raw_tx: mock_raw_tx_bytes_indexer_test() },
                ),
            ),
        }],
    }
}

struct MockServiceController {
    event_tx: broadcast::Sender<Arc<VerifiedCheckpoint>>,
    latest_seq: Arc<AtomicU64>,
    checkpoint_cache: Arc<Mutex<HashMap<CheckpointSequenceNumber, Arc<VerifiedCheckpoint>>>>,
    subscriber_connected_notify: Arc<Notify>,
}

impl MockServiceController {
    async fn wait_for_subscriber(&self) {
        self.subscriber_connected_notify.notified().await;
    }

    async fn publish_checkpoints_up_to(&self, val: u64) {
        let last_known = self.latest_seq.swap(val, Ordering::SeqCst);
        if val > last_known {
            let mut cache_guard = self.checkpoint_cache.lock().await;
            for seq_to_publish in (last_known + 1)..=val {
                let checkpoint = cache_guard
                    .entry(seq_to_publish)
                    .or_insert_with(|| {
                        Arc::new(create_mock_verified_checkpoint_for_indexer(
                            seq_to_publish,
                            1,
                        ))
                    })
                    .clone();

                if self.event_tx.send(checkpoint).is_err() {
                    warn!(
                        "[MockServiceController-IndexerTest] Failed to send checkpoint {} event, no active subscribers.",
                        seq_to_publish
                    );
                } else {
                    info!(
                        "[MockServiceController-IndexerTest] Successfully sent checkpoint {} event.",
                        seq_to_publish
                    );
                }
            }
        }
    }
}

async fn start_mock_grpc_server(
    initial_latest_seq: CheckpointSequenceNumber,
) -> Result<(SocketAddr, MockServiceController), anyhow::Error> {
    let initial_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(initial_addr).await?;
    let actual_addr = listener.local_addr()?;

    let (event_tx, _event_rx) = broadcast::channel::<Arc<VerifiedCheckpoint>>(100);
    let checkpoint_cache = Arc::new(Mutex::new(HashMap::new()));
    let contents_cache = Arc::new(Mutex::new(HashMap::new()));

    let service = MockCheckpointService::new(
        event_tx.clone(),
        initial_latest_seq,
        checkpoint_cache.clone(),
        contents_cache.clone(),
    );
    let subscriber_connected_notify_clone = service.subscriber_connected_notify.clone();

    let server_builder =
        Server::builder().add_service(CheckpointGprcServiceServer::new(service.clone()));

    tokio::spawn(async move {
        if let Err(e) = server_builder
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            error!(
                "[MockCheckpointService-IndexerTest] Mock gRPC server error: {:?}",
                e
            );
        }
    });
    // Add a small delay to allow the mock gRPC server to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok((
        actual_addr,
        MockServiceController {
            event_tx,
            latest_seq: service.latest_sequence_number.clone(),
            checkpoint_cache,
            subscriber_connected_notify: subscriber_connected_notify_clone,
        },
    ))
}

#[tokio::test]
async fn test_indexer_grpc_streaming_ingestion() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    let start_checkpoint_seq = 5u64;
    let num_checkpoints_to_stream = 3u64;
    let highest_checkpoint_to_stream = start_checkpoint_seq + num_checkpoints_to_stream - 1;

    let (mock_server_addr, mock_service_controller) =
        start_mock_grpc_server(start_checkpoint_seq - 1).await?;

    {
        let mut cache = mock_service_controller.checkpoint_cache.lock().await;
        for i in 0..=highest_checkpoint_to_stream {
            cache.insert(
                i,
                Arc::new(create_mock_verified_checkpoint_for_indexer(i, 1)),
            );
        }
    }

    let db_url = common::get_indexer_db_url(Some("grpc_streaming_test_db"));
    info!("[IndexerTest] Using DB URL: {}", db_url);

    let writer_config = IndexerTypeConfig::writer_mode(
        Some(SnapshotLagConfig::default()),
        None,
        true,
        Some(start_checkpoint_seq),
        Some(mock_server_addr.to_string()),
    );

    let cancellation_token = CancellationToken::new();
    let dummy_fullnode_rpc_url = "http://127.0.0.1:12345".to_string();
    let (pg_store, indexer_handle) = start_test_indexer_impl(
        db_url.clone(),
        true,
        None,
        dummy_fullnode_rpc_url,
        writer_config,
        None,
        cancellation_token.clone(),
    )
    .await;
    info!(
        "[IndexerTest] Indexer started, gRPC streaming to {}",
        mock_server_addr
    );

    info!("[IndexerTest] Waiting for gRPC subscriber to connect...");
    mock_service_controller.wait_for_subscriber().await;
    info!("[IndexerTest] gRPC subscriber connected.");

    info!(
        "[IndexerTest] Triggering mock service to publish checkpoints from {} to {}...",
        start_checkpoint_seq, highest_checkpoint_to_stream
    );
    mock_service_controller
        .publish_checkpoints_up_to(highest_checkpoint_to_stream)
        .await;
    info!("[IndexerTest] Mock service publish triggered.");

    let mut conn = pg_store.blocking_cp().get().map_err(|e| {
        error!(
            "[IndexerTest] Failed to get DB connection from pool: {:?}",
            e
        );
        anyhow::anyhow!("DB connection error: {:?}", e)
    })?;

    let mut processed_count = 0;
    let start_time = Instant::now();
    let timeout_duration = Duration::from_secs(15);

    while Instant::now().duration_since(start_time) < timeout_duration {
        let count: i64 = checkpoints
            .count()
            .get_result(&mut conn)
            .expect("Failed to query checkpoint count");

        processed_count = count;
        if count >= num_checkpoints_to_stream as i64 {
            info!("[IndexerTest] Indexer processed {} checkpoints.", count);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    if processed_count < num_checkpoints_to_stream as i64 {
        error!(
            "[IndexerTest] Timeout: Indexer only processed {} out of {} expected checkpoints within {:?}.",
            processed_count, num_checkpoints_to_stream, timeout_duration
        );
    }

    let stored_checkpoints_db: Vec<StoredCheckpoint> =
        checkpoints.order(sequence_number.asc()).load(&mut conn)?;

    assert_eq!(
        stored_checkpoints_db.len() as u64,
        num_checkpoints_to_stream,
        "Did not find the expected number of checkpoints in the DB."
    );

    for i in 0..num_checkpoints_to_stream {
        let expected_seq = start_checkpoint_seq + i;
        let db_checkpoint = &stored_checkpoints_db[i as usize];
        assert_eq!(db_checkpoint.sequence_number as u64, expected_seq);
        assert_eq!(
            db_checkpoint.epoch as u64,
            // Epoch in mock_checkpoint_data_gprc_indexer_test is `sequence_number_val` (or 0 if
            // seq is 0) create_mock_verified_checkpoint_for_indexer uses epoch 1 for
            // non-zero seq. The StoredCheckpoint gets epoch from IndexedCheckpoint,
            // which gets it from CertifiedCheckpointSummary.
            // The CertifiedCheckpointSummary from mock_checkpoint_data_gprc_indexer_test sets
            // epoch = sequence_number_val (or 0).
            if expected_seq == 0 { 0 } else { expected_seq }
        );
        info!(
            "[IndexerTest] Stored checkpoint for seq {}: {:?}",
            expected_seq, db_checkpoint
        );
        info!(
            "[IndexerTest] Stored checkpoint_digest for seq {}: {:?}",
            expected_seq, db_checkpoint.checkpoint_digest
        );

        // Construct the CheckpointSummary as the indexer would see it, derived from
        // gRPC and conversion defaults.
        let expected_summary_epoch = if expected_seq == 0 { 0 } else { expected_seq };
        let expected_summary_network_total_tx = 100 + expected_seq;
        let expected_summary_content_digest = CheckpointContentsDigest::new([0u8; 32]);
        let expected_summary_previous_digest = if expected_seq > 0 {
            Some(CheckpointDigest::new([(expected_seq - 1) as u8; 32]))
        } else {
            None
        };

        let summary_to_digest = CheckpointSummary {
            epoch: expected_summary_epoch,
            sequence_number: expected_seq,
            network_total_transactions: expected_summary_network_total_tx,
            content_digest: expected_summary_content_digest,
            previous_digest: expected_summary_previous_digest,
            epoch_rolling_gas_cost_summary: GasCostSummary::default(),
            timestamp_ms: 0, // Fixed for deterministic digest, matching gRPC conversion default
            checkpoint_commitments: Vec::new(), // Default
            end_of_epoch_data: None, // Default
            version_specific_data: Vec::new(), // Default
        };
        let expected_checkpoint_digest = summary_to_digest.digest();

        info!(
            "[IndexerTest] Expected summary_digest for seq {}: {:?}",
            expected_seq,
            expected_checkpoint_digest.into_inner().to_vec()
        );

        assert_eq!(
            db_checkpoint.checkpoint_digest,
            expected_checkpoint_digest.into_inner().to_vec(),
            "Stored checkpoint digest does not match actual generated content digest"
        );
    }
    info!(
        "[IndexerTest] Successfully verified {} checkpoints in DB.",
        num_checkpoints_to_stream
    );

    info!("[IndexerTest] Shutting down indexer...");
    cancellation_token.cancel();
    let _ = timeout(Duration::from_secs(5), indexer_handle).await??;
    info!("[IndexerTest] Indexer shutdown complete.");

    Ok(())
}
