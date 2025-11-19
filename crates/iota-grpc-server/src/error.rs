// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_types::base_types::ObjectID;
use tonic::{Code, Status};

// TODO: We can remove them when we define the ErrorReason proto
/// Error reasons for field violations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorReason {
    /// Field is missing when it's required
    FieldMissing,
    /// Field value is invalid
    FieldInvalid,
    /// Field value is out of range
    OutOfRange,
}

impl ErrorReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorReason::FieldMissing => "FIELD_MISSING",
            ErrorReason::FieldInvalid => "FIELD_INVALID",
            ErrorReason::OutOfRange => "OUT_OF_RANGE",
        }
    }
}

// TODO: We can remove them when we include the google FieldViolation proto
/// Field violation error information
#[derive(Debug, Clone)]
pub struct FieldViolation {
    field: String,
    description: Option<String>,
    reason: Option<ErrorReason>,
}

impl FieldViolation {
    /// Create a new field violation for the given field
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            description: None,
            reason: None,
        }
    }

    /// Create a field violation for an array element at a specific index
    pub fn new_at(field: impl Into<String>, index: usize) -> Self {
        Self::new(format!("{}[{}]", field.into(), index))
    }

    /// Set the description of what went wrong
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the reason code for this violation
    pub fn with_reason(mut self, reason: ErrorReason) -> Self {
        self.reason = Some(reason);
        self
    }

    /// Nest this field violation under a parent field at a specific index
    pub fn nested_at(mut self, parent_field: &str, index: usize) -> Self {
        self.field = format!("{parent_field}[{index}].{field}", field = self.field);
        self
    }
}

// TODO: Replace these placeholders with actual Google proto types when
// available These should come from google.rpc proto definitions
pub type ErrorInfo = ();
pub type BadRequest = ();
pub type RetryInfo = ();

/// Main RPC error type
///
/// An error encountered while serving an RPC request.
/// The main purpose of this error type is to provide a convenient type for
/// converting between internal errors and a response that needs to be sent to a
/// calling client.
#[derive(Debug)]
pub struct RpcError {
    code: Code,
    message: Option<String>,
    details: Option<Box<ErrorDetails>>,
}

/// Result type using RpcError as the error type
pub type Result<T> = std::result::Result<T, RpcError>;

impl RpcError {
    pub fn new<T: Into<String>>(code: Code, message: T) -> Self {
        Self {
            code,
            message: Some(message.into()),
            details: None,
        }
    }

    // TODO: This can be removed when we defined error protos and their conversions
    pub fn field_violation(violation: FieldViolation) -> Self {
        let message = if let Some(desc) = &violation.description {
            format!("invalid {}: {}", violation.field, desc)
        } else {
            format!("invalid {}", violation.field)
        };

        Self {
            code: Code::InvalidArgument,
            message: Some(message),
            details: None,
        }
    }

    pub fn not_found() -> Self {
        Self {
            code: Code::NotFound,
            message: None,
            details: None,
        }
    }

    pub fn into_status_proto(self) -> iota_grpc_types::v0::google::rpc::Status {
        iota_grpc_types::v0::google::rpc::Status {
            code: self.code.into(),
            message: self.message.unwrap_or_default(),
            details: self
                .details
                .map(ErrorDetails::into_status_details)
                .unwrap_or_default(),
        }
    }
}

// TODO: This can be removed when we defined error protos and their conversions
impl From<RpcError> for Status {
    fn from(error: RpcError) -> Self {
        use prost::Message;

        let code = error.code;
        let status = error.into_status_proto();
        let details = status.encode_to_vec().into();
        let message = status.message;

        Status::with_details(code, message, details)
    }
}

// TODO: This can be removed when we defined error protos and their conversions
impl From<FieldViolation> for RpcError {
    fn from(violation: FieldViolation) -> Self {
        RpcError::field_violation(violation)
    }
}

impl From<anyhow::Error> for RpcError {
    fn from(value: anyhow::Error) -> Self {
        Self {
            code: Code::Internal,
            message: Some(value.to_string()),
            details: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ErrorDetails {
    error_info: Option<ErrorInfo>,
    bad_request: Option<BadRequest>,
    retry_info: Option<RetryInfo>,
}

impl ErrorDetails {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn error_info(&self) -> Option<&ErrorInfo> {
        self.error_info.as_ref()
    }

    pub fn bad_request(&self) -> Option<&BadRequest> {
        self.bad_request.as_ref()
    }

    pub fn retry_info(&self) -> Option<&RetryInfo> {
        self.retry_info.as_ref()
    }

    pub fn details(&self) -> &[prost_types::Any] {
        &[]
    }

    pub fn with_bad_request(mut self, bad_request: BadRequest) -> Self {
        self.bad_request = Some(bad_request);
        self
    }

    #[allow(clippy::boxed_local)]
    fn into_status_details(self: Box<Self>) -> Vec<prost_types::Any> {
        // TODO: Implement proper conversion when Google proto types are available
        // This should convert error_info, bad_request, and retry_info to
        // prost_types::Any
        vec![]
    }
}

// TODO: The following errors also defined in iota-rest-api, but we want to
// avoid depending on the iota-rest-api crate, hence we duplicate them here.
#[derive(Debug, Clone)]
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
        Self::new(tonic::Code::NotFound, value.to_string())
    }
}
