// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Shared conversion utilities between different gRPC API types and IOTA types.
//! This module eliminates code duplication in bidirectional conversions.

use iota_types::{
    base_types::{ObjectDigest, ObjectID, SequenceNumber},
    error::{IotaError, IotaObjectResponseError, UserInputError},
};

use crate::node::{
    DeletedError, DisplayError, DynamicFieldNotFoundError, NotExistsError, ObjectError,
    UnknownError, object_error::Error as ObjectErrorVariant,
};

/// Convert ObjectID to gRPC Address
pub fn object_id_to_grpc_address(object_id: ObjectID) -> crate::common::Address {
    crate::common::Address {
        address: object_id.into_bytes().to_vec(),
    }
}

/// Convert optional gRPC Address to ObjectID
pub fn grpc_address_to_object_id(
    address: Option<crate::common::Address>,
) -> anyhow::Result<ObjectID> {
    let addr = address.ok_or_else(|| anyhow::anyhow!("Missing object ID in address"))?;
    if addr.address.len() != 32 {
        return Err(anyhow::anyhow!(
            "Invalid object ID length: {}, expected 32",
            addr.address.len()
        ));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&addr.address);
    ObjectID::from_bytes(bytes).map_err(|e| anyhow::anyhow!("Invalid object ID: {e}"))
}

/// Convert ObjectDigest to gRPC Digest
pub fn object_digest_to_grpc_digest(digest: ObjectDigest) -> crate::common::Digest {
    crate::common::Digest {
        digest: digest.into_inner().to_vec(),
    }
}

/// Convert optional gRPC Digest to ObjectDigest
pub fn grpc_digest_to_object_digest(
    digest: Option<crate::common::Digest>,
) -> anyhow::Result<ObjectDigest> {
    let digest = digest.ok_or_else(|| anyhow::anyhow!("Missing digest"))?;
    ObjectDigest::try_from(digest.digest.as_slice())
        .map_err(|e| anyhow::anyhow!("Invalid digest: {e}"))
}

/// Convert IotaObjectResponseError to gRPC ObjectError
pub fn iota_object_response_error_to_grpc(error: IotaObjectResponseError) -> ObjectError {
    let error_variant = match error {
        IotaObjectResponseError::NotExists { object_id } => {
            ObjectErrorVariant::NotExists(NotExistsError {
                object_id: Some(object_id_to_grpc_address(object_id)),
            })
        }
        IotaObjectResponseError::DynamicFieldNotFound { parent_object_id } => {
            ObjectErrorVariant::DynamicFieldNotFound(DynamicFieldNotFoundError {
                parent_object_id: Some(object_id_to_grpc_address(parent_object_id)),
            })
        }
        IotaObjectResponseError::Deleted {
            object_id,
            version,
            digest,
        } => ObjectErrorVariant::Deleted(DeletedError {
            object_id: Some(object_id_to_grpc_address(object_id)),
            version: version.value(),
            digest: Some(object_digest_to_grpc_digest(digest)),
        }),
        IotaObjectResponseError::Unknown => ObjectErrorVariant::Unknown(UnknownError {}),
        IotaObjectResponseError::Display { error } => ObjectErrorVariant::Display(DisplayError {
            error_message: error,
        }),
    };

    ObjectError {
        error: Some(error_variant),
    }
}

/// Convert UserInputError to gRPC ObjectError
pub fn user_input_error_to_grpc(error: UserInputError) -> ObjectError {
    let error_variant = match error {
        UserInputError::ObjectNotFound { object_id, .. } => {
            // For GetObject API, we always convert ObjectNotFound to NotExists
            // since we don't support version parameters
            ObjectErrorVariant::NotExists(NotExistsError {
                object_id: Some(object_id_to_grpc_address(object_id)),
            })
        }
        UserInputError::ObjectDeleted { object_ref } => ObjectErrorVariant::Deleted(DeletedError {
            object_id: Some(object_id_to_grpc_address(object_ref.0)),
            version: object_ref.1.value(),
            digest: Some(object_digest_to_grpc_digest(object_ref.2)),
        }),
        _ => ObjectErrorVariant::Unknown(UnknownError {}),
    };

    ObjectError {
        error: Some(error_variant),
    }
}

/// Convert IotaError to gRPC ObjectError
pub fn iota_error_to_grpc(error: IotaError) -> ObjectError {
    match error {
        IotaError::IotaObjectResponse { error } => iota_object_response_error_to_grpc(error),
        IotaError::UserInput { error } => user_input_error_to_grpc(error),
        _ => ObjectError {
            error: Some(ObjectErrorVariant::Unknown(UnknownError {})),
        },
    }
}

/// Convert gRPC ObjectError to IotaObjectResponseError
pub fn grpc_to_iota_object_response_error(
    error: ObjectError,
) -> anyhow::Result<IotaObjectResponseError> {
    let error_variant = error
        .error
        .ok_or_else(|| anyhow::anyhow!("Missing error variant"))?;

    match error_variant {
        ObjectErrorVariant::NotExists(not_exists) => {
            let object_id = grpc_address_to_object_id(not_exists.object_id)?;
            Ok(IotaObjectResponseError::NotExists { object_id })
        }
        ObjectErrorVariant::DynamicFieldNotFound(dynamic_field) => {
            let parent_object_id = grpc_address_to_object_id(dynamic_field.parent_object_id)?;
            Ok(IotaObjectResponseError::DynamicFieldNotFound { parent_object_id })
        }
        ObjectErrorVariant::Deleted(deleted) => {
            let object_id = grpc_address_to_object_id(deleted.object_id)?;
            let version = SequenceNumber::from_u64(deleted.version);
            let digest = grpc_digest_to_object_digest(deleted.digest)?;
            Ok(IotaObjectResponseError::Deleted {
                object_id,
                version,
                digest,
            })
        }
        ObjectErrorVariant::Unknown(_unknown) => Ok(IotaObjectResponseError::Unknown),
        ObjectErrorVariant::Display(display) => Ok(IotaObjectResponseError::Display {
            error: display.error_message,
        }),
    }
}
