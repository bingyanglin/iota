// Placeholder for gRPC error handling

use anyhow;
use thiserror::Error; // To use anyhow::Error in the enum

#[derive(Debug, Error)]
pub enum GrpcApiError {
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Conversion error: {0}")]
    ConversionError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Underlying system error: {0}")] // New variant for anyhow::Error
    SystemError(#[from] anyhow::Error),
    // Add other error types as needed
}

// Implement From<iota_types::error::IotaError> if needed
// impl From<iota_types::error::IotaError> for GrpcApiError { ... }

impl From<GrpcApiError> for tonic::Status {
    fn from(err: GrpcApiError) -> Self {
        match err {
            GrpcApiError::NotFound(msg) => tonic::Status::not_found(msg),
            GrpcApiError::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
            GrpcApiError::ConversionError(msg) => {
                tonic::Status::internal(format!("Conversion failed: {msg}"))
            }
            GrpcApiError::SerializationError(msg) => {
                tonic::Status::internal(format!("Serialization failed: {msg}"))
            }
            GrpcApiError::DeserializationError(msg) => {
                tonic::Status::internal(format!("Deserialization failed: {msg}"))
            }
            GrpcApiError::SystemError(err) => {
                // Handle new variant
                tonic::Status::internal(format!("System error: {err}"))
            }
            GrpcApiError::Internal(msg) => tonic::Status::internal(msg),
        }
    }
}
