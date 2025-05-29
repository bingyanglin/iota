// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::SocketAddr,
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
    test_utils::{IndexerTypeConfig, TestDatabase, start_test_indexer_impl},
};
use iota_types::{
    base_types::{
        AuthorityName, IotaAddress, ObjectID, ObjectRef, SequenceNumber as TxSequenceNumber,
    },
    crypto::{AuthoritySignInfo, AuthoritySignature},
    digests::{CheckpointContentsDigest, CheckpointDigest, ObjectDigest},
    gas::GasCostSummary,
    message_envelope::Envelope,
    messages_checkpoint::{CheckpointSequenceNumber, CheckpointSummary, SignedCheckpointSummary},
    transaction::TransactionData,
};
use secrecy::{ExposeSecret, Secret};
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use tracing_subscriber;

// --- Mock gRPC Service Implementation ---
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

#[derive(Debug, Default)]
struct MockCheckpointService {
    start_seq: CheckpointSequenceNumber,
    num_to_stream: u64,
}

impl MockCheckpointService {
    fn new(start_seq: CheckpointSequenceNumber, num_to_stream: u64) -> Self {
        Self {
            start_seq,
            num_to_stream,
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
        // For this integration test, we only care about streaming.
        // Unary calls can return unimplemented or a dummy response if needed.
        println!(
            "MockService: Received GetCheckpointFull for id: {}",
            request.into_inner().checkpoint_id
        );
        Err(Status::unimplemented(
            "get_checkpoint_full not needed for this test",
        ))
    }

    async fn get_checkpoint(
        &self,
        _request: Request<GetCheckpointRequest>,
    ) -> Result<Response<SignedCheckpointSummaryGprc>, Status> {
        Err(Status::unimplemented(
            "get_checkpoint not implemented in mock",
        ))
    }

    async fn list_checkpoints(
        &self,
        _request: Request<ListCheckpointsRequest>,
    ) -> Result<Response<iota_grpc_api::proto::iota::gprc::v1::CheckpointPageGprc>, Status> {
        Err(Status::unimplemented(
            "list_checkpoints not implemented in mock",
        ))
    }

    async fn subscribe_new_checkpoints(
        &self,
        request: Request<SubscribeNewCheckpointsRequest>,
    ) -> Result<Response<Self::SubscribeNewCheckpointsStream>, Status> {
        println!(
            "MockService: Received SubscribeNewCheckpoints: {:?}",
            request.get_ref()
        );
        let req_inner = request.into_inner();
        let effective_start_seq = req_inner
            .start_from_checkpoint_sequence_number
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(self.start_seq); // Use requested start, or mock's default if None

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let num_items_to_stream = self.num_to_stream; // Use the configured number of items to stream

        tokio::spawn(async move {
            for i in 0..num_items_to_stream {
                let current_seq = effective_start_seq + i;
                let gprc_data = mock_checkpoint_data_gprc_indexer_test(current_seq);
                let streamed_item = StreamedCheckpoint {
                    checkpoint_type: Some(streamed_checkpoint::CheckpointType::FullData(gprc_data)),
                };
                if tx.send(Ok(streamed_item)).await.is_err() {
                    println!(
                        "MockService: Stream receiver dropped for seq {}",
                        current_seq
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            println!(
                "MockService: Finished streaming {} mock checkpoints starting from {}.",
                num_items_to_stream, effective_start_seq
            );
            // Sender tx is dropped here, closing the stream for the client.
        });

        Ok(Response::new(ReceiverStream::new(rx)))
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

    // This is the digest that represents the content (transactions, effects) of the
    // checkpoint. In our test, we assume these are all zero/default, leading to
    // a specific digest.
    let mock_content_digest_bytes = CheckpointContentsDigest::new([0u8; 32])
        .into_inner()
        .to_vec();

    CheckpointDataGprc {
        summary: Some(SignedCheckpointSummaryGprc {
            epoch: epoch_val,
            sequence_number: sequence_number_val,
            network_total_transactions: network_total_transactions_val,
            content_digest: Some(CheckpointDigestGprc {
                digest: mock_content_digest_bytes, // Use the pre-determined content digest
            }),
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

async fn start_mock_grpc_server(
    service: MockCheckpointService,
) -> Result<SocketAddr, anyhow::Error> {
    let initial_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(initial_addr).await?;
    let actual_addr = listener.local_addr()?;

    let server_builder = Server::builder().add_service(CheckpointGprcServiceServer::new(service));

    tokio::spawn(async move {
        if let Err(e) = server_builder
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            eprintln!("Mock gRPC server error: {:?}", e);
        }
    });
    Ok(actual_addr)
}

// --- Integration Test ---
#[tokio::test]
async fn test_indexer_grpc_streaming_ingestion() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for test output
    // Use `try_init` to avoid panic if already initialized globally or by another
    // test
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    // 1. Setup Mock gRPC Server
    let start_checkpoint_seq = 5u64;
    let num_checkpoints_to_stream = 3u64;
    let mock_service = MockCheckpointService::new(start_checkpoint_seq, num_checkpoints_to_stream);
    let server_addr = start_mock_grpc_server(mock_service).await?;
    let grpc_url = format!("grpc://{}", server_addr);
    println!("Mock gRPC server started at: {}", grpc_url);

    // 2. Setup Test Database
    let db_name = "test_indexer_grpc_streaming";
    let db_url_base = std::env::var("CI_TEST_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgrespw@localhost:5432".into());
    let db_url_with_name = Secret::new(format!("{}/{}", db_url_base, db_name));
    let mut test_db = TestDatabase::new(db_url_with_name.clone());
    test_db.recreate(); // Drops if exists and creates a new one
    println!("Test database {} created/reset.", db_name);

    // 3. Start Indexer Writer with gRPC streaming enabled
    let cancel = CancellationToken::new();
    let indexer_type_config = IndexerTypeConfig::writer_mode(
        None, // Default snapshot config
        None, // No epoch pruning for this test
        true, // Enable gRPC streaming
        Some(start_checkpoint_seq),
    );

    let (pg_store, indexer_handle) = start_test_indexer_impl(
        db_url_with_name.expose_secret().to_string(),
        true, // <--- Set to true to ensure migrations run
        None, // No db_init_hook
        grpc_url.clone(), /* This is the rpc_client_url, which is *different* from
               * remote_store_url for checkpoints */
        indexer_type_config,
        None, // data_ingestion_path - set to None so remote_store_url is attempted
        cancel.clone(),
    )
    .await;

    // 4. Wait for Indexer to Ingest Data and Verify
    println!("Waiting for checkpoints to be ingested...");
    let verification_timeout = Duration::from_secs(5); // Total timeout for verification
    let poll_interval = Duration::from_millis(200);

    let checkpoints_to_verify = vec![
        start_checkpoint_seq,
        start_checkpoint_seq + 1,
        start_checkpoint_seq + 2,
    ]; // 5, 6, 7
    for seq_to_check in checkpoints_to_verify {
        let mut found = false;
        let start_time = Instant::now();
        while !found && start_time.elapsed() < verification_timeout {
            let mut conn_pooled = pg_store
                .blocking_cp()
                .get()
                .expect("Failed to get DB connection for verification");
            let conn: &mut diesel::PgConnection = &mut *conn_pooled;

            let checkpoint_from_db: Option<StoredCheckpoint> = checkpoints
                .filter(sequence_number.eq(seq_to_check as i64))
                .first::<StoredCheckpoint>(conn)
                .optional()?;

            if checkpoint_from_db.is_some() {
                if let Some(summary_db) = checkpoint_from_db {
                    assert_eq!(summary_db.sequence_number, seq_to_check as i64);

                    // --- Reconstruct the expected SignedCheckpointSummary and its digest ---
                    // --- This assumes the indexer is using *signed_checkpoint_summary.digest() ---
                    let expected_epoch = if seq_to_check == 0 { 0 } else { seq_to_check };
                    let expected_network_total_tx = 100 + seq_to_check;

                    let expected_previous_digest_opt = if seq_to_check > 0 {
                        Some(CheckpointDigest::new([(seq_to_check - 1) as u8; 32]))
                    } else {
                        None
                    };

                    // This must match the content_digest used by the indexer when it forms
                    // its CheckpointSummary from the gRPC message.
                    // The mock sends CheckpointContentsDigest::new([0u8; 32]) as the
                    // content_digest.
                    let expected_content_digest = CheckpointContentsDigest::new([0u8; 32]);

                    let expected_checkpoint_summary_data = CheckpointSummary {
                        epoch: expected_epoch,
                        sequence_number: seq_to_check,
                        network_total_transactions: expected_network_total_tx,
                        content_digest: expected_content_digest,
                        previous_digest: expected_previous_digest_opt,
                        epoch_rolling_gas_cost_summary: GasCostSummary::default(),
                        timestamp_ms: 0, // Assuming default in conversion by indexer
                        checkpoint_commitments: Vec::new(), // Assuming default
                        end_of_epoch_data: None, // Assuming default
                        version_specific_data: Vec::new(), // Assuming default
                    };

                    let dummy_auth_sig_info = AuthoritySignInfo {
                        epoch: expected_epoch,
                        authority: AuthorityName::ZERO, // Default/zero authority name
                        signature: AuthoritySignature::default(), // Default signature
                    };

                    let expected_envelope: SignedCheckpointSummary = // Alias for Envelope<CheckpointSummary, AuthoritySignInfo>
                        Envelope::new_from_data_and_sig(expected_checkpoint_summary_data, dummy_auth_sig_info);

                    let expected_digest_from_envelope = expected_envelope.digest();
                    let expected_digest_vec: Vec<u8> =
                        expected_digest_from_envelope.into_inner().to_vec();
                    // --- End of full envelope digest calculation ---

                    assert_eq!(
                        summary_db.checkpoint_digest, expected_digest_vec,
                        "Checkpoint digest mismatch for checkpoint {}. DB_VALUE (left) vs EXPECTED_FULL_ENVELOPE_DIGEST (right)",
                        seq_to_check
                    );
                    println!(
                        "Successfully verified checkpoint {} in the database.",
                        seq_to_check
                    );
                    found = true;
                    break;
                }
            } else {
                // println!("Checkpoint {} not found yet, polling...",
                // seq_to_check);
            }
            tokio::time::sleep(poll_interval).await;
        }

        if !found {
            panic!(
                "Timeout: Checkpoint {} was not found in the DB after 5s.",
                seq_to_check
            );
        }
    }
    println!(
        "Successfully verified all {} expected checkpoints in the database.",
        num_checkpoints_to_stream
    );

    // 6. Shutdown and Cleanup
    println!("Shutting down indexer...");
    cancel.cancel();
    match timeout(Duration::from_secs(5), indexer_handle).await {
        Ok(Ok(Ok(_))) => println!("Indexer shut down gracefully."),
        Ok(Ok(Err(e))) => eprintln!("Indexer exited with error: {:?}", e),
        Ok(Err(e)) => eprintln!("Indexer join error: {:?}", e),
        Err(_) => eprintln!("Indexer shutdown timed out."),
    }

    drop(test_db);
    println!("Test database dropped.");

    Ok(())
}
