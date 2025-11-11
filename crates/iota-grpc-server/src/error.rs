// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use tonic::Status;

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

    /// Convert to a google.rpc.Status proto
    pub fn into_status_proto(self) -> iota_grpc_types::v0::google_rpc::Status {
        let message = if let Some(desc) = self.description {
            format!("Field '{}': {}", self.field, desc)
        } else {
            format!("Field '{}' violation", self.field)
        };

        iota_grpc_types::v0::google_rpc::Status {
            code: 3, // INVALID_ARGUMENT
            message,
            details: vec![],
        }
    }
}

impl From<FieldViolation> for RpcError {
    fn from(violation: FieldViolation) -> Self {
        RpcError::InvalidArgument(vec![violation])
    }
}

/// Main RPC error type
#[derive(Debug, Clone)]
pub enum RpcError {
    /// Invalid argument error with field violations
    InvalidArgument(Vec<FieldViolation>),
    /// Resource not found
    NotFound(String),
    /// Internal server error
    Internal(String),
    /// Unimplemented feature
    Unimplemented(String),
}

/// Result type using RpcError as the error type
pub type Result<T> = std::result::Result<T, RpcError>;

impl RpcError {
    pub fn field_violation(violation: FieldViolation) -> Self {
        RpcError::InvalidArgument(vec![violation])
    }
}

impl From<RpcError> for Status {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::InvalidArgument(violations) => {
                let message = if violations.len() == 1 {
                    let v = &violations[0];
                    if let Some(desc) = &v.description {
                        format!("invalid {}: {}", v.field, desc)
                    } else {
                        format!("invalid {}", v.field)
                    }
                } else {
                    format!("{} invalid fields", violations.len())
                };

                Status::invalid_argument(message)
            }
            RpcError::NotFound(message) => Status::not_found(message),
            RpcError::Internal(message) => Status::internal(message),
            RpcError::Unimplemented(message) => Status::unimplemented(message),
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::InvalidArgument(violations) => {
                write!(f, "Invalid argument: {} violations", violations.len())
            }
            RpcError::NotFound(msg) => write!(f, "Not found: {}", msg),
            RpcError::Internal(msg) => write!(f, "Internal error: {}", msg),
            RpcError::Unimplemented(msg) => write!(f, "Unimplemented: {}", msg),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<anyhow::Error> for RpcError {
    fn from(error: anyhow::Error) -> Self {
        RpcError::Internal(format!("{value}", value = error))
    }
}
