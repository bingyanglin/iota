// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Constants for gRPC server configuration.

/// Default maximum message size for chunked responses (4MB)
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Minimum allowed message size (1MB)
pub const MIN_MESSAGE_SIZE: usize = 1024 * 1024;

/// Maximum allowed message size (128MB)
pub const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;
