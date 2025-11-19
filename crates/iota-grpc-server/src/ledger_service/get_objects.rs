// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    merge::Merge,
    v0::{
        ledger_service::{GetObjectsRequest, GetObjectsResponse, ObjectResult},
        object::Object,
    },
};
use iota_types::base_types::ObjectID;
use prost::Message;
use prost_types::FieldMask;

use crate::{
    constants::{DEFAULT_MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE},
    error::{ErrorReason, FieldViolation, ObjectNotFoundError, RpcError},
    types::GrpcReader,
};

pub const READ_MASK_DEFAULT: &str = "reference.object_id,reference.version,reference.digest";

type ValidationResult = Result<(Vec<(ObjectID, Option<u64>)>, FieldMaskTree), RpcError>;

pub fn validate_get_object_requests(
    requests: Vec<(Option<String>, Option<u64>)>,
    read_mask: Option<FieldMask>,
) -> ValidationResult {
    let read_mask = {
        let read_mask = read_mask.unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        read_mask.validate::<Object>().map_err(|path| {
            FieldViolation::new("read_mask")
                .with_description(format!("invalid read_mask path: {path}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;
        FieldMaskTree::from(read_mask)
    };
    let requests = requests
        .into_iter()
        .enumerate()
        .map(|(idx, (object_id, version))| {
            let object_id = object_id
                .as_ref()
                .ok_or_else(|| {
                    FieldViolation::new("object_id")
                        .with_reason(ErrorReason::FieldMissing)
                        .nested_at("requests", idx)
                })?
                .parse()
                .map_err(|e| {
                    FieldViolation::new("object_id")
                        .with_description(format!("invalid object_id: {e}"))
                        .with_reason(ErrorReason::FieldInvalid)
                        .nested_at("requests", idx)
                })?;
            Ok((object_id, version))
        })
        .collect::<Result<_, RpcError>>()?;
    Ok((requests, read_mask))
}

#[tracing::instrument(skip(reader))]
pub(crate) fn get_objects(
    reader: GrpcReader,
    GetObjectsRequest {
        requests,
        read_mask,
        max_message_size_bytes,
        ..
    }: GetObjectsRequest,
) -> Result<Vec<GetObjectsResponse>, RpcError> {
    let requests = requests
        .map(|r| r.requests)
        .unwrap_or_default()
        .into_iter()
        .map(|req| {
            let object_ref = req.object_ref;
            (
                object_ref.as_ref().and_then(|r| r.object_id.clone()),
                object_ref.and_then(|r| r.version),
            )
        })
        .collect();
    let (requests, read_mask) = validate_get_object_requests(requests, read_mask)?;

    // Validate and set max_message_size
    let max_message_size = match max_message_size_bytes {
        Some(size) => {
            let size = usize::try_from(size).map_err(|_| {
                FieldViolation::new("max_message_size_bytes")
                    .with_description("must be a valid positive integer")
                    .with_reason(ErrorReason::FieldInvalid)
            })?;

            match size {
                s if s < MIN_MESSAGE_SIZE => {
                    return Err(FieldViolation::new("max_message_size_bytes")
                        .with_description(format!("must be at least {MIN_MESSAGE_SIZE} bytes"))
                        .with_reason(ErrorReason::FieldInvalid)
                        .into());
                }
                s if s > MAX_MESSAGE_SIZE => {
                    return Err(FieldViolation::new("max_message_size_bytes")
                        .with_description(format!("must be at most {MAX_MESSAGE_SIZE} bytes"))
                        .with_reason(ErrorReason::FieldInvalid)
                        .into());
                }
                s => s,
            }
        }
        None => DEFAULT_MAX_MESSAGE_SIZE,
    };

    // Create an iterator that lazily fetches objects
    let mut objects_iter = requests
        .into_iter()
        .map(move |(object_id, version)| get_object_impl(&reader, object_id, version, &read_mask))
        .map(|result| match result {
            Ok(object) => ObjectResult::new_object(object),
            Err(error) => ObjectResult::new_error(error.into_status_proto()),
        })
        .peekable();

    // Track if we've returned at least one response
    let mut returned_response = false;

    // Stream responses on-demand using the iterator
    Ok(std::iter::from_fn(move || {
        let mut current_batch = Vec::new();
        let mut current_size = 0;

        // Fill the current batch up to max_message_size
        loop {
            // Peek at the next object to check if it fits
            let next_size = objects_iter.peek().map(|obj| obj.encoded_len());

            match next_size {
                Some(object_size) => {
                    // Check if adding this object would exceed the limit
                    if current_size + object_size > max_message_size && !current_batch.is_empty() {
                        // Current batch is full, return it
                        // has_next is true because we peeked and found another object
                        returned_response = true;
                        return Some(GetObjectsResponse {
                            objects: current_batch,
                            has_next: true,
                        });
                    }

                    // Object fits, consume it and add to batch
                    let object_result = objects_iter.next().unwrap();
                    current_batch.push(object_result);
                    current_size += object_size;
                }
                None => {
                    // No more objects
                    if !current_batch.is_empty() {
                        returned_response = true;
                        return Some(GetObjectsResponse {
                            objects: current_batch,
                            has_next: false,
                        });
                    } else if !returned_response {
                        // Return empty response if we haven't returned anything yet
                        returned_response = true;
                        return Some(GetObjectsResponse {
                            objects: vec![],
                            has_next: false,
                        });
                    } else {
                        // We've already returned responses, stop iteration
                        return None;
                    }
                }
            }
        }
    })
    .collect())
}

#[tracing::instrument(skip(reader))]
fn get_object_impl(
    reader: &GrpcReader,
    object_id: ObjectID,
    version: Option<u64>,
    read_mask: &FieldMaskTree,
) -> Result<Object, RpcError> {
    let object = if let Some(version) = version {
        reader
            .get_object_by_key(&object_id, version.into())
            .ok_or_else(|| ObjectNotFoundError::new_with_version(object_id, version))?
    } else {
        reader
            .get_object(&object_id)
            .ok_or_else(|| ObjectNotFoundError::new(object_id))?
    };

    let mut message = Object::default();

    Merge::merge(&mut message, &object, read_mask);

    Ok(message)
}
