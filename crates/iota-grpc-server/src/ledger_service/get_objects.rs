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
use tonic::Status;

use crate::{
    constants::{DEFAULT_MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE},
    error::{ErrorReason, FieldViolation, RpcError},
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
        // TODO: Add read_mask validation once Object implements MessageFields
        // read_mask.validate::<Object>().map_err(|path| {
        //     FieldViolation::new("read_mask")
        //         .with_description(format!("invalid read_mask path: {path}"))
        //         .with_reason(ErrorReason::FieldInvalid)
        // })?;
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
) -> Result<Vec<GetObjectsResponse>, Status> {
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
    let (requests, read_mask) =
        validate_get_object_requests(requests, read_mask).map_err(Status::from)?;

    // Validate and set max_message_size
    let max_message_size = match max_message_size_bytes {
        Some(size) => {
            let size = usize::try_from(size).map_err(|_| {
                Status::invalid_argument("max_message_size_bytes must be a valid positive integer")
            })?;

            match size {
                s if s < MIN_MESSAGE_SIZE => {
                    return Err(Status::invalid_argument(format!(
                        "max_message_size_bytes must be at least {MIN_MESSAGE_SIZE} bytes"
                    )));
                }
                s if s > MAX_MESSAGE_SIZE => {
                    return Err(Status::invalid_argument(format!(
                        "max_message_size_bytes must be at most {MAX_MESSAGE_SIZE} bytes"
                    )));
                }
                s => s,
            }
        }
        None => DEFAULT_MAX_MESSAGE_SIZE,
    };

    let objects: Vec<ObjectResult> = requests
        .into_iter()
        .map(|(object_id, version)| get_object_impl(&reader, object_id, version, &read_mask))
        .map(|result| match result {
            Ok(object) => ObjectResult::new_object(object),
            Err(error) => ObjectResult::new_error(error.into_status_proto()),
        })
        .collect();

    // Chunk the results based on max_message_size
    let mut responses = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_size = 0;

    for object_result in objects {
        let object_size = object_result.encoded_len();

        if current_size + object_size > max_message_size && !current_batch.is_empty() {
            // Current batch is full, create a response
            responses.push(GetObjectsResponse {
                objects: std::mem::take(&mut current_batch),
                has_next: true,
            });
            current_size = 0;
        }

        current_batch.push(object_result);
        current_size += object_size;
    }

    // Add the final batch
    if !current_batch.is_empty() {
        responses.push(GetObjectsResponse {
            objects: current_batch,
            has_next: false,
        });
    }

    // If there are no results, return a single empty response
    if responses.is_empty() {
        responses.push(GetObjectsResponse {
            objects: vec![],
            has_next: false,
        });
    } else {
        // Ensure the last response has has_next = false
        if let Some(last) = responses.last_mut() {
            last.has_next = false;
        }
    }

    Ok(responses)
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

#[derive(Debug, Clone)]
struct ObjectNotFoundError {
    object_id: ObjectID,
    version: Option<u64>,
}

impl ObjectNotFoundError {
    fn new(object_id: ObjectID) -> Self {
        Self {
            object_id,
            version: None,
        }
    }

    fn new_with_version(object_id: ObjectID, version: u64) -> Self {
        Self {
            object_id,
            version: Some(version),
        }
    }
}

impl std::fmt::Display for ObjectNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.version {
            Some(version) => {
                write!(
                    f,
                    "Object {} at version {} not found",
                    self.object_id, version
                )
            }
            None => write!(f, "Object {} not found", self.object_id),
        }
    }
}

impl std::error::Error for ObjectNotFoundError {}

impl From<ObjectNotFoundError> for RpcError {
    fn from(value: ObjectNotFoundError) -> Self {
        Self::NotFound(value.to_string())
    }
}
