// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use prost_types::FieldMask;

use crate::v0::{
    epoch::Epoch,
    ledger_service::{
        GetEpochRequest, GetEpochResponse, GetObjectsRequest, GetObjectsResponse,
        GetTransactionsRequest, GetTransactionsResponse, ObjectRequest, ObjectRequests,
        ObjectResult, TransactionRequest, TransactionRequests, TransactionResult,
    },
    types::{Digest, ObjectReference},
};

impl GetEpochRequest {
    pub fn latest() -> Self {
        Self {
            epoch: None,
            read_mask: None,
        }
    }

    pub fn new(epoch: u64) -> Self {
        Self {
            epoch: Some(epoch),
            read_mask: None,
        }
    }

    pub fn with_read_mask<T: Into<FieldMask>>(mut self, field: T) -> Self {
        self.read_mask = Some(field.into());
        self
    }
}

impl GetEpochResponse {
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch: Some(epoch) }
    }
}

impl ObjectRequest {
    pub fn new(object_id: &str) -> Self {
        Self {
            object_ref: Some(ObjectReference {
                object_id: Some(object_id.to_string()),
                version: None,
                digest: None,
            }),
        }
    }

    pub fn with_version(mut self, version: u64) -> Self {
        if let Some(ref mut obj_ref) = self.object_ref {
            obj_ref.version = Some(version);
        }
        self
    }
}

impl GetObjectsRequest {
    pub fn new(requests: Vec<ObjectRequest>) -> Self {
        Self {
            requests: Some(ObjectRequests { requests }),
            read_mask: None,
            max_message_size_bytes: None,
        }
    }

    pub fn with_read_mask<T: Into<FieldMask>>(mut self, field: T) -> Self {
        self.read_mask = Some(field.into());
        self
    }

    pub fn with_max_message_size_bytes(mut self, size: u32) -> Self {
        self.max_message_size_bytes = Some(size);
        self
    }
}

impl GetObjectsResponse {
    pub fn new(objects: Vec<ObjectResult>) -> Self {
        Self {
            objects,
            has_next: false,
        }
    }
}

impl ObjectResult {
    pub fn new_object(object: crate::v0::object::Object) -> Self {
        Self {
            result: Some(crate::v0::ledger_service::object_result::Result::Object(
                object,
            )),
        }
    }

    pub fn new_error(error: crate::v0::google::rpc::Status) -> Self {
        Self {
            result: Some(crate::v0::ledger_service::object_result::Result::Error(
                error,
            )),
        }
    }

    pub fn object(&self) -> &crate::v0::object::Object {
        match &self.result {
            Some(crate::v0::ledger_service::object_result::Result::Object(obj)) => obj,
            _ => panic!("ObjectResult does not contain an object"),
        }
    }
}

impl TransactionRequest {
    pub fn new(digest: Vec<u8>) -> Self {
        Self {
            digest: Some(Digest {
                digest: digest.into(),
            }),
        }
    }
}

impl GetTransactionsRequest {
    pub fn new(requests: Vec<TransactionRequest>) -> Self {
        Self {
            requests: Some(TransactionRequests { requests }),
            read_mask: None,
            max_message_size_bytes: None,
        }
    }

    pub fn with_read_mask<T: Into<FieldMask>>(mut self, field: T) -> Self {
        self.read_mask = Some(field.into());
        self
    }

    pub fn with_max_message_size_bytes(mut self, size: u32) -> Self {
        self.max_message_size_bytes = Some(size);
        self
    }
}

impl GetTransactionsResponse {
    pub fn new(transactions: Vec<TransactionResult>, has_next: bool) -> Self {
        Self {
            transactions,
            has_next,
        }
    }
}

impl TransactionResult {
    pub fn new_transaction(transaction: crate::v0::transaction::ExecutedTransaction) -> Self {
        Self {
            result: Some(
                crate::v0::ledger_service::transaction_result::Result::Transaction(Box::new(
                    transaction,
                )),
            ),
        }
    }

    pub fn new_error(error: crate::v0::google::rpc::Status) -> Self {
        Self {
            result: Some(crate::v0::ledger_service::transaction_result::Result::Error(error)),
        }
    }

    pub fn transaction(&self) -> &crate::v0::transaction::ExecutedTransaction {
        match &self.result {
            Some(crate::v0::ledger_service::transaction_result::Result::Transaction(tx)) => tx,
            _ => panic!("TransactionResult does not contain a transaction"),
        }
    }
}
