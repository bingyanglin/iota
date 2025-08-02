// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// BCS-compatible event structures for gRPC serialization
use iota_json_rpc_types::IotaEvent;
use iota_types::{
    base_types::{IotaAddress, ObjectID, TransactionDigest},
    event::EventID,
};
use move_core_types::{identifier::Identifier, language_storage::StructTag};
use serde::{Deserialize, Serialize};
use serde_json;

/// BCS-compatible version of IotaEvent without JSON-specific serde annotations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BcsIotaEvent {
    pub id: BcsEventID,
    pub package_id: ObjectID,
    pub transaction_module: Identifier,
    pub sender: IotaAddress,
    pub type_: StructTag,
    pub parsed_json: String, // Store JSON as string for BCS compatibility
    pub bcs_data: Vec<u8>,
    pub timestamp_ms: Option<u64>,
}

/// BCS-compatible version of EventID
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BcsEventID {
    pub tx_digest: TransactionDigest,
    pub event_seq: u64,
}

impl From<IotaEvent> for BcsIotaEvent {
    fn from(event: IotaEvent) -> Self {
        Self {
            id: BcsEventID {
                tx_digest: event.id.tx_digest,
                event_seq: event.id.event_seq,
            },
            package_id: event.package_id,
            transaction_module: event.transaction_module,
            sender: event.sender,
            type_: event.type_,
            parsed_json: event.parsed_json.to_string(),
            bcs_data: event.bcs.into_bytes(),
            timestamp_ms: event.timestamp_ms,
        }
    }
}

impl From<BcsIotaEvent> for IotaEvent {
    fn from(bcs_event: BcsIotaEvent) -> Self {
        Self {
            id: EventID {
                tx_digest: bcs_event.id.tx_digest,
                event_seq: bcs_event.id.event_seq,
            },
            package_id: bcs_event.package_id,
            transaction_module: bcs_event.transaction_module,
            sender: bcs_event.sender,
            type_: bcs_event.type_,
            parsed_json: serde_json::from_str(&bcs_event.parsed_json).unwrap_or_default(),
            bcs: iota_json_rpc_types::BcsEvent::new(bcs_event.bcs_data),
            timestamp_ms: bcs_event.timestamp_ms,
        }
    }
}
