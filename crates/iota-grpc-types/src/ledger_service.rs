// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use prost_types::FieldMask;

use crate::v0::{
    epoch::Epoch,
    ledger_service::{GetEpochRequest, GetEpochResponse},
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
