// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    v0::{
        bcs::BcsData,
        google_rpc::Status as RpcStatus,
        ledger_service::{GetObjectsRequest, GetObjectsResponse, ObjectRequest, ObjectResult},
        object::Object,
        types::{Digest, ObjectReference},
    },
};
use iota_types::{base_types::ObjectID, object::Object as IotaObject};
use prost_types::FieldMask;
use tonic::Status;

use crate::types::GrpcReader;

/// Default fields to read if no mask is specified
pub const READ_MASK_DEFAULT: &str = "reference.object_id,reference.version,reference.digest";

/// Error type for object not found
#[derive(Debug)]
pub struct ObjectNotFoundError {
    object_id: ObjectID,
    version: Option<u64>,
}

impl ObjectNotFoundError {
    pub fn new(object_id: ObjectID) -> Self {
        Self {
            object_id,
            version: None,
        }
    }

    pub fn new_with_version(object_id: ObjectID, version: u64) -> Self {
        Self {
            object_id,
            version: Some(version),
        }
    }
}

impl std::fmt::Display for ObjectNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.version {
            Some(v) => write!(f, "Object {} at version {} not found", self.object_id, v),
            None => write!(f, "Object {} not found", self.object_id),
        }
    }
}

impl std::error::Error for ObjectNotFoundError {}

impl From<ObjectNotFoundError> for Status {
    fn from(value: ObjectNotFoundError) -> Self {
        Status::not_found(value.to_string())
    }
}

/// Convert IOTA object to gRPC Object with field masking
fn object_to_proto(object: &IotaObject, read_mask: &FieldMaskTree) -> Object {
    let mut message = Object::default();

    // Handle reference fields
    if read_mask.contains("reference") {
        let mut reference = ObjectReference::default();

        if read_mask.contains("reference.object_id") {
            reference.object_id = Some(object.id().to_string());
        }

        if read_mask.contains("reference.version") {
            reference.version = Some(object.version().value());
        }

        if read_mask.contains("reference.digest") {
            reference.digest = Some(Digest {
                digest: object.digest().inner().to_vec(),
            });
        }

        message.reference = Some(reference);
    }

    // Handle BCS field
    if read_mask.contains("bcs") {
        if let Ok(bcs_bytes) = bcs::to_bytes(&object) {
            message.bcs = Some(BcsData { data: bcs_bytes });
        }
    }

    message
}

/// Get multiple objects with streaming response
#[tracing::instrument(skip(reader))]
pub async fn get_objects(
    reader: GrpcReader,
    request: GetObjectsRequest,
) -> Result<GetObjectsResponse, Status> {
    // Parse field mask (without validation for now, as field constants aren't
    // generated yet)
    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        FieldMaskTree::from(read_mask)
    };

    let requests = request.requests.map(|r| r.requests).unwrap_or_default();

    let mut results = Vec::new();

    for object_req in requests {
        let result = get_single_object(&reader, object_req, &read_mask).await;
        results.push(result);
    }

    Ok(GetObjectsResponse { objects: results })
}

/// Get a single object and convert to ObjectResult
async fn get_single_object(
    reader: &GrpcReader,
    request: ObjectRequest,
    read_mask: &FieldMaskTree,
) -> ObjectResult {
    let object_ref = match request.object_ref {
        Some(ref obj_ref) => obj_ref,
        None => {
            let error = Status::invalid_argument("object_ref is required");
            return ObjectResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::object_result::Result::Error(RpcStatus {
                        code: error.code() as i32,
                        message: error.message().to_string(),
                        details: vec![],
                    }),
                ),
            };
        }
    };

    // Parse object_id
    let object_id = match object_ref.object_id.as_ref() {
        Some(id_str) => match id_str.parse::<ObjectID>() {
            Ok(id) => id,
            Err(_) => {
                let error = Status::invalid_argument(format!("Invalid object_id: {id_str}"));
                return ObjectResult {
                    result: Some(
                        iota_grpc_types::v0::ledger_service::object_result::Result::Error(
                            RpcStatus {
                                code: error.code() as i32,
                                message: error.message().to_string(),
                                details: vec![],
                            },
                        ),
                    ),
                };
            }
        },
        None => {
            let error = Status::invalid_argument("object_id is required");
            return ObjectResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::object_result::Result::Error(RpcStatus {
                        code: error.code() as i32,
                        message: error.message().to_string(),
                        details: vec![],
                    }),
                ),
            };
        }
    };

    // Get object from storage
    let object = match reader.get_object(&object_id) {
        Some(obj) => obj,
        None => {
            let error: Status = ObjectNotFoundError::new(object_id).into();
            return ObjectResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::object_result::Result::Error(RpcStatus {
                        code: error.code() as i32,
                        message: error.message().to_string(),
                        details: vec![],
                    }),
                ),
            };
        }
    };

    // If a specific version was requested, verify it matches
    if let Some(requested_version) = object_ref.version {
        if object.version().value() != requested_version {
            let error: Status =
                ObjectNotFoundError::new_with_version(object_id, requested_version).into();
            return ObjectResult {
                result: Some(
                    iota_grpc_types::v0::ledger_service::object_result::Result::Error(RpcStatus {
                        code: error.code() as i32,
                        message: error.message().to_string(),
                        details: vec![],
                    }),
                ),
            };
        }
    }

    // Convert to proto with field masking
    let proto_object = object_to_proto(&object, read_mask);

    ObjectResult {
        result: Some(
            iota_grpc_types::v0::ledger_service::object_result::Result::Object(proto_object),
        ),
    }
}
