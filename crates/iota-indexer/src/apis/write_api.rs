// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use fastcrypto::encoding::Base64;
use futures::{FutureExt, TryFutureExt};
use iota_grpc_client::Client as GrpcClient;
use iota_grpc_types::field::{FieldMask, FieldMaskUtil};
use iota_json::IotaJsonValue;
use iota_json_rpc::IotaRpcModule;
use iota_json_rpc_api::WriteApiServer;
use iota_json_rpc_types::{
    DevInspectArgs, DevInspectResults, DryRunTransactionBlockResponse, IotaMoveViewCallResults,
    IotaTransactionBlock, IotaTransactionBlockEffects, IotaTransactionBlockResponse,
    IotaTransactionBlockResponseOptions, IotaTypeTag, MoveFunctionName,
};
use iota_open_rpc::Module;
use iota_package_resolver::{PackageStore, Resolver};
use iota_protocol_config::Chain;
use iota_transaction_builder::TransactionBuilder;
use iota_types::{
    base_types::IotaAddress,
    effects::{TransactionEffects, TransactionEvents},
    iota_serde::BigInt,
    quorum_driver_types::ExecuteTransactionRequestType,
    signature::GenericSignature,
    transaction::{SenderSignedData, TransactionData},
};
use jsonrpsee::{RpcModule, core::RpcResult, http_client::HttpClient};

use crate::{
    apis::error::Error as ApiError,
    errors::{IndexerError, IndexerResult},
    ingestion::primary::prepare::InMemTxChanges,
    metrics::IndexerMetrics,
    models::transactions::tx_events_to_iota_tx_events,
    optimistic_indexing::OptimisticTransactionExecutor,
    read::IndexerReader,
    store::package_resolver::IndexerStorePackageResolver,
    types::{IotaTransactionBlockResponseWithOptions, grpc_conversion},
};

// As an optimization, we're trying to request only the fields we actually need.
const DRY_RUN_TRANSACTION_READ_MASK: &[&str] = &[
    "executed_transaction.signatures.bcs",
    "executed_transaction.effects.bcs",
    "executed_transaction.events.events.bcs",
    "executed_transaction.input_objects.bcs",
    "executed_transaction.output_objects.bcs",
    "suggested_gas_price",
    "execution_result.execution_error.source",
];

#[derive(Clone)]
pub struct WriteApi {
    fullnode_grpc_client: GrpcClient,
    transaction_builder: TransactionBuilder,
    package_resolver: Arc<Resolver<IndexerStorePackageResolver>>,
}

#[derive(Clone)]
pub struct OptimisticWriteApi {
    write_api: WriteApi,
    optimistic_tx_executor: OptimisticTransactionExecutor,
}

impl WriteApi {
    pub fn new(fullnode_grpc_client: GrpcClient, reader: IndexerReader) -> Self {
        let package_resolver = IndexerStorePackageResolver::new(reader.get_pool());
        let data_reader = Arc::new(reader);
        Self {
            fullnode_grpc_client,
            transaction_builder: TransactionBuilder::new(data_reader),
            package_resolver: Arc::new(Resolver::new(package_resolver)),
        }
    }

    async fn dry_run_transaction_block_impl(
        &self,
        tx_bytes: Base64,
        package_resolver: &Arc<Resolver<impl PackageStore>>,
    ) -> IndexerResult<DryRunTransactionBlockResponse> {
        let transaction_data = bcs::from_bytes::<TransactionData>(&tx_bytes.to_vec()?)?;
        let tx_digest = transaction_data.digest();

        let readmask = FieldMask::from_paths(DRY_RUN_TRANSACTION_READ_MASK)
            .display()
            .to_string();

        let simulate_tx_response = self
            .fullnode_grpc_client
            .simulate_transaction(
                transaction_data.clone().try_into()?,
                false,
                false,
                Some(readmask.as_str()),
            )
            .await?;

        let executed_transaction = simulate_tx_response.executed_transaction()?;
        let execution_error_source = simulate_tx_response
            .execution_error()
            .and_then(|e| e.source.clone());
        let suggested_gas_price = simulate_tx_response.suggested_gas_price;

        let input_objects = grpc_conversion::objects(executed_transaction.input_objects()?)?;
        let output_objects = grpc_conversion::objects(executed_transaction.output_objects()?)?;

        let objects = input_objects
            .iter()
            .chain(output_objects.iter())
            .collect::<Vec<_>>();

        let tx_effects: TransactionEffects =
            executed_transaction.effects()?.effects()?.try_into()?;

        let tx_signatures = executed_transaction
            .signatures()?
            .signatures
            .iter()
            .map(|s| -> IndexerResult<_> { Ok(GenericSignature::try_from(s.signature()?)?) })
            .collect::<IndexerResult<Vec<GenericSignature>>>()?;

        let sender_signed_data = SenderSignedData::new(transaction_data.clone(), tx_signatures);

        let tx_events = TransactionEvents::try_from(executed_transaction.events()?.events()?)?;

        let in_mem_tx_changes =
            InMemTxChanges::new(&objects, IndexerMetrics::new(&Default::default()));

        // as a minor optimization we will run concurrently the following four futures
        let fut1 = in_mem_tx_changes
            .get_changes(&transaction_data, &tx_effects, &tx_digest)
            .map_ok(|(balance_changes, object_changes)| {
                (
                    balance_changes,
                    object_changes
                        .into_iter()
                        .map(iota_json_rpc_types::ObjectChange::from)
                        .collect::<Vec<_>>(),
                )
            });

        let fut2 = IotaTransactionBlock::try_from_with_package_resolver(
            sender_signed_data,
            package_resolver,
            tx_digest,
        )
        .map_err(Into::into);

        // timestamp is None because it represent a checkpoint one, on a dry run
        // operation we don't have this information.
        let fut3 = tx_events_to_iota_tx_events(tx_events, package_resolver, tx_digest, None);

        let fut4 = IotaTransactionBlockEffects::from_native_with_clever_error(
            tx_effects.clone(),
            package_resolver,
        )
        .map(Ok);

        let ((balance_changes, object_changes), transaction_block, events, effects) =
            futures::future::try_join4(fut1, fut2, fut3, fut4).await?;

        Ok(DryRunTransactionBlockResponse {
            effects,
            events,
            object_changes,
            balance_changes,
            input: transaction_block.data,
            suggested_gas_price,
            execution_error_source,
        })
    }
}

impl OptimisticWriteApi {
    pub fn new(write_api: WriteApi, optimistic_tx_executor: OptimisticTransactionExecutor) -> Self {
        Self {
            write_api,
            optimistic_tx_executor,
        }
    }

    pub fn fullnode_client(&self) -> &HttpClient {
        // with the use of gRPC API we need to make a distinction between the fullnode
        // and the indexer ReadApi::is_transaction_indexed_on_node.
        //
        // returning the HttpClient is not feasible anymore, also this method is only
        // used on the graphql side to access only one ReadApi method call.
        // Since the Indexer's ReadApi can directly invoke the
        // is_transaction_indexed_on_node with either JSON RPC or gRPC, we could
        // deprecate this method in favor of storing the Indexer's ReadApi in the
        // graphql context data, the same way we do for the indexer's WriteApi.
        //
        // will be resolved as part of issue: https://github.com/iotaledger/iota/issues/7926
        todo!()
    }
}

#[async_trait]
impl WriteApiServer for WriteApi {
    /// This method will always return an error. The user shall use the
    /// [`OptimisticWriteApi`] to execute transactions.
    async fn execute_transaction_block(
        &self,
        _tx_bytes: Base64,
        _signatures: Vec<Base64>,
        _options: Option<IotaTransactionBlockResponseOptions>,
        _request_type: Option<ExecuteTransactionRequestType>,
    ) -> RpcResult<IotaTransactionBlockResponse> {
        Err(IndexerError::Generic(
            "execute_transaction_block should be called from OptimisticWriteApi".into(),
        )
        .into())
    }

    async fn dev_inspect_transaction_block(
        &self,
        _sender_address: IotaAddress,
        _tx_bytes: Base64,
        _gas_price: Option<BigInt<u64>>,
        _epoch: Option<BigInt<u64>>,
        _additional_args: Option<DevInspectArgs>,
    ) -> RpcResult<DevInspectResults> {
        todo!("waiting issue: #10390 and #10391 to be resolved");
    }

    async fn dry_run_transaction_block(
        &self,
        tx_bytes: Base64,
    ) -> RpcResult<DryRunTransactionBlockResponse> {
        self.dry_run_transaction_block_impl(tx_bytes, &self.package_resolver)
            .await
            .map_err(Into::into)
    }

    async fn view_function_call(
        &self,
        function_name: String,
        type_args: Option<Vec<IotaTypeTag>>,
        arguments: Vec<IotaJsonValue>,
    ) -> RpcResult<IotaMoveViewCallResults> {
        let MoveFunctionName {
            package,
            module,
            function,
        } = function_name.as_str().parse().map_err(IndexerError::from)?;
        let sender = IotaAddress::ZERO;
        let tx_kind = self
            .transaction_builder
            .move_view_call_tx_kind(
                package,
                &module,
                &function,
                type_args.unwrap_or_default(),
                arguments,
            )
            .await
            .map_err(IndexerError::from)?;
        let tx_bytes = Base64::from_bytes(&bcs::to_bytes(&tx_kind).map_err(IndexerError::from)?);
        let dev_inspect_results = self
            .dev_inspect_transaction_block(sender, tx_bytes, None, None, None)
            .await?;
        Ok(IotaMoveViewCallResults::from_dev_inspect_results(
            self.package_resolver.package_store().clone(),
            dev_inspect_results,
        )
        .await
        .map_err(IndexerError::from)?)
    }
}

#[async_trait]
impl WriteApiServer for OptimisticWriteApi {
    async fn execute_transaction_block(
        &self,
        tx_bytes: Base64,
        signatures: Vec<Base64>,
        options: Option<IotaTransactionBlockResponseOptions>,
        _request_type: Option<ExecuteTransactionRequestType>,
    ) -> RpcResult<IotaTransactionBlockResponse> {
        let iota_transaction_response = self
            .optimistic_tx_executor
            .execute_and_index_transaction(tx_bytes, signatures, options.clone())
            .await?;
        Ok(IotaTransactionBlockResponseWithOptions {
            response: iota_transaction_response,
            options: options.unwrap_or_default(),
        }
        .into())
    }

    async fn dev_inspect_transaction_block(
        &self,
        sender_address: IotaAddress,
        tx_bytes: Base64,
        gas_price: Option<BigInt<u64>>,
        epoch: Option<BigInt<u64>>,
        additional_args: Option<DevInspectArgs>,
    ) -> RpcResult<DevInspectResults> {
        self.write_api
            .dev_inspect_transaction_block(
                sender_address,
                tx_bytes,
                gas_price,
                epoch,
                additional_args,
            )
            .await
    }

    async fn dry_run_transaction_block(
        &self,
        tx_bytes: Base64,
    ) -> RpcResult<DryRunTransactionBlockResponse> {
        self.write_api.dry_run_transaction_block(tx_bytes).await
    }

    async fn view_function_call(
        &self,
        function_name: String,
        type_args: Option<Vec<IotaTypeTag>>,
        arguments: Vec<IotaJsonValue>,
    ) -> RpcResult<IotaMoveViewCallResults> {
        let chain = self
            .optimistic_tx_executor
            .read
            .get_chain_identifier_in_blocking_task()
            .await?
            .chain();
        if !matches!(chain, Chain::Unknown) {
            return Err(ApiError::UnsupportedFeature(format!(
                "View calls are not yet supported on {}",
                chain.as_str()
            ))
            .into());
        }

        self.write_api
            .view_function_call(function_name, type_args, arguments)
            .await
    }
}

impl IotaRpcModule for WriteApi {
    fn rpc(self) -> RpcModule<Self> {
        self.into_rpc()
    }

    fn rpc_doc_module() -> Module {
        iota_json_rpc_api::WriteApiOpenRpc::module_doc()
    }
}

impl IotaRpcModule for OptimisticWriteApi {
    fn rpc(self) -> RpcModule<Self> {
        self.into_rpc()
    }

    fn rpc_doc_module() -> Module {
        iota_json_rpc_api::WriteApiOpenRpc::module_doc()
    }
}
