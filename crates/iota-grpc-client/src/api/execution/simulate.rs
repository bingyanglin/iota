// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! High-level API for transaction simulation.

use iota_grpc_types::v0::transaction_execution_service::{
    SimulateTransactionRequest, simulate_transaction_request::TransactionCheckModes,
};
use iota_sdk_types::Transaction;

use crate::{
    Client,
    api::{
        EXECUTION_READ_MASK, FieldMask, FieldMaskUtil, Result, TransactionSimulationResponse,
        TryFromProtoError, build_proto_transaction, extract_execution_data,
    },
};

impl Client {
    /// Simulate a transaction without executing it.
    ///
    /// This allows you to preview the effects of a transaction before
    /// actually submitting it to the network.
    ///
    /// Set `dev_inspect` to true for relaxed Move VM checks (useful for
    /// debugging and development).
    ///
    /// Returns `TransactionSimulationResponse` containing the simulated
    /// effects, events, and input/output objects.
    ///
    /// # Field Mask
    ///
    /// The optional `read_mask` parameter controls which fields the server
    /// returns. If `None`, uses [`EXECUTION_READ_MASK`] which includes effects,
    /// events, and input/output objects.
    ///
    /// **Required fields** (must be included in custom masks):
    /// - `transaction.effects` - Transaction effects
    ///
    /// **Optional fields:**
    /// - `transaction.events` - Transaction events
    /// - `transaction.input_objects` - Input objects used
    /// - `transaction.output_objects` - Output objects created/modified
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use iota_grpc_client::Client;
    /// # use iota_sdk_types::Transaction;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("http://localhost:9000").await?;
    ///
    /// let tx: Transaction = todo!();
    ///
    /// // Standard simulation with all fields
    /// let result = client.simulate_transaction(tx.clone(), false, None).await?;
    /// println!("Simulation status: {:?}", result.effects.status());
    ///
    /// // Dev-inspect mode with minimal fields
    /// let result = client
    ///     .simulate_transaction(tx, true, Some("transaction.effects"))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`EXECUTION_READ_MASK`]: crate::EXECUTION_READ_MASK
    pub async fn simulate_transaction(
        &self,
        transaction: Transaction,
        dev_inspect: bool,
        read_mask: Option<&str>,
    ) -> Result<TransactionSimulationResponse> {
        // Build proto transaction directly from SDK types
        let proto_transaction = build_proto_transaction(&transaction, transaction.digest())?;

        let tx_checks = if dev_inspect {
            vec![TransactionCheckModes::DisableVmChecks as i32]
        } else {
            vec![]
        };

        let mask = read_mask.unwrap_or(EXECUTION_READ_MASK);
        let request = SimulateTransactionRequest {
            transaction: Some(proto_transaction),
            tx_checks,
            estimate_gas_budget: None,
            read_mask: Some(FieldMask::from_str(mask)),
        };

        let mut client = self.execution_service_client();

        let response = client.simulate_transaction(request).await?.into_inner();

        let executed = response
            .transaction
            .ok_or(TryFromProtoError::missing("transaction"))?;

        Ok(extract_execution_data(&executed)?.into())
    }
}
