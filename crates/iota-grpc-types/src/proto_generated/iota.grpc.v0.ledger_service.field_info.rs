// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod _field_impls {
    #![allow(clippy::wrong_self_convention)]
    use super::*;
    use crate::field::MessageFields;
    use crate::field::MessageField;
    #[allow(unused_imports)]
    use crate::v0::epoch::Epoch;
    #[allow(unused_imports)]
    use crate::v0::epoch::EpochFieldPathBuilder;
    impl GetEpochRequest {
        pub const EPOCH_FIELD: &'static MessageField = &MessageField {
            name: "epoch",
            json_name: "epoch",
            number: 1i32,
            message_fields: None,
        };
        pub const READ_MASK_FIELD: &'static MessageField = &MessageField {
            name: "read_mask",
            json_name: "readMask",
            number: 2i32,
            message_fields: None,
        };
    }
    impl MessageFields for GetEpochRequest {
        const FIELDS: &'static [&'static MessageField] = &[
            Self::EPOCH_FIELD,
            Self::READ_MASK_FIELD,
        ];
    }
    impl GetEpochRequest {
        pub fn path_builder() -> GetEpochRequestFieldPathBuilder {
            GetEpochRequestFieldPathBuilder::new()
        }
    }
    pub struct GetEpochRequestFieldPathBuilder {
        path: Vec<&'static str>,
    }
    impl GetEpochRequestFieldPathBuilder {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            Self { path: Default::default() }
        }
        #[doc(hidden)]
        pub fn new_with_base(base: Vec<&'static str>) -> Self {
            Self { path: base }
        }
        pub fn finish(self) -> String {
            self.path.join(".")
        }
        pub fn epoch(mut self) -> String {
            self.path.push(GetEpochRequest::EPOCH_FIELD.name);
            self.finish()
        }
        pub fn read_mask(mut self) -> String {
            self.path.push(GetEpochRequest::READ_MASK_FIELD.name);
            self.finish()
        }
    }
    impl GetEpochResponse {
        pub const EPOCH_FIELD: &'static MessageField = &MessageField {
            name: "epoch",
            json_name: "epoch",
            number: 1i32,
            message_fields: Some(Epoch::FIELDS),
        };
    }
    impl MessageFields for GetEpochResponse {
        const FIELDS: &'static [&'static MessageField] = &[Self::EPOCH_FIELD];
    }
    impl GetEpochResponse {
        pub fn path_builder() -> GetEpochResponseFieldPathBuilder {
            GetEpochResponseFieldPathBuilder::new()
        }
    }
    pub struct GetEpochResponseFieldPathBuilder {
        path: Vec<&'static str>,
    }
    impl GetEpochResponseFieldPathBuilder {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            Self { path: Default::default() }
        }
        #[doc(hidden)]
        pub fn new_with_base(base: Vec<&'static str>) -> Self {
            Self { path: base }
        }
        pub fn finish(self) -> String {
            self.path.join(".")
        }
        pub fn epoch(mut self) -> EpochFieldPathBuilder {
            self.path.push(GetEpochResponse::EPOCH_FIELD.name);
            EpochFieldPathBuilder::new_with_base(self.path)
        }
    }
}
pub use _field_impls::*;
