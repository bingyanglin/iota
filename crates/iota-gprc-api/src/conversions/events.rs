// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_types::{digests::TransactionDigest, event::Event as CoreEvent};

use crate::{error::GrpcApiError, proto::iota::gprc::v1::TransactionEventGprc};

pub fn convert_core_event_to_gprc(
    transaction_digest: &TransactionDigest,
    event_index: u32,
    core_event: &CoreEvent,
) -> Result<TransactionEventGprc, GrpcApiError> {
    Ok(TransactionEventGprc {
        transaction_digest_hex: format!("{:#x}", transaction_digest),
        event_index,
        event_type_tag: core_event.type_.to_string(),
        event_contents: core_event.contents.clone(),
    })
}
