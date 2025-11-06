// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod _field_impls {
    #![allow(clippy::wrong_self_convention)]
    use super::*;
    use crate::field::MessageFields;
    use crate::field::MessageField;
    #[allow(unused_imports)]
    use crate::v0::dynamic_field::DynamicField;
    #[allow(unused_imports)]
    use crate::v0::dynamic_field::DynamicFieldFieldPathBuilder;
    impl ListDynamicFieldsRequest {
        pub const PARENT_FIELD: &'static MessageField = &MessageField {
            name: "parent",
            json_name: "parent",
            number: 1i32,
            message_fields: None,
        };
        pub const READ_MASK_FIELD: &'static MessageField = &MessageField {
            name: "read_mask",
            json_name: "readMask",
            number: 2i32,
            message_fields: None,
        };
        pub const MAX_MESSAGE_SIZE_BYTES_FIELD: &'static MessageField = &MessageField {
            name: "max_message_size_bytes",
            json_name: "maxMessageSizeBytes",
            number: 3i32,
            message_fields: None,
        };
    }
    impl MessageFields for ListDynamicFieldsRequest {
        const FIELDS: &'static [&'static MessageField] = &[
            Self::PARENT_FIELD,
            Self::READ_MASK_FIELD,
            Self::MAX_MESSAGE_SIZE_BYTES_FIELD,
        ];
    }
    impl ListDynamicFieldsRequest {
        pub fn path_builder() -> ListDynamicFieldsRequestFieldPathBuilder {
            ListDynamicFieldsRequestFieldPathBuilder::new()
        }
    }
    pub struct ListDynamicFieldsRequestFieldPathBuilder {
        path: Vec<&'static str>,
    }
    impl ListDynamicFieldsRequestFieldPathBuilder {
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
        pub fn parent(mut self) -> String {
            self.path.push(ListDynamicFieldsRequest::PARENT_FIELD.name);
            self.finish()
        }
        pub fn read_mask(mut self) -> String {
            self.path.push(ListDynamicFieldsRequest::READ_MASK_FIELD.name);
            self.finish()
        }
        pub fn max_message_size_bytes(mut self) -> String {
            self.path.push(ListDynamicFieldsRequest::MAX_MESSAGE_SIZE_BYTES_FIELD.name);
            self.finish()
        }
    }
    impl ListDynamicFieldsResponse {
        pub const DYNAMIC_FIELDS_FIELD: &'static MessageField = &MessageField {
            name: "dynamic_fields",
            json_name: "dynamicFields",
            number: 1i32,
            message_fields: Some(DynamicField::FIELDS),
        };
        pub const HAS_NEXT_FIELD: &'static MessageField = &MessageField {
            name: "has_next",
            json_name: "hasNext",
            number: 2i32,
            message_fields: None,
        };
    }
    impl MessageFields for ListDynamicFieldsResponse {
        const FIELDS: &'static [&'static MessageField] = &[
            Self::DYNAMIC_FIELDS_FIELD,
            Self::HAS_NEXT_FIELD,
        ];
    }
    impl ListDynamicFieldsResponse {
        pub fn path_builder() -> ListDynamicFieldsResponseFieldPathBuilder {
            ListDynamicFieldsResponseFieldPathBuilder::new()
        }
    }
    pub struct ListDynamicFieldsResponseFieldPathBuilder {
        path: Vec<&'static str>,
    }
    impl ListDynamicFieldsResponseFieldPathBuilder {
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
        pub fn dynamic_fields(mut self) -> DynamicFieldFieldPathBuilder {
            self.path.push(ListDynamicFieldsResponse::DYNAMIC_FIELDS_FIELD.name);
            DynamicFieldFieldPathBuilder::new_with_base(self.path)
        }
        pub fn has_next(mut self) -> String {
            self.path.push(ListDynamicFieldsResponse::HAS_NEXT_FIELD.name);
            self.finish()
        }
    }
}
pub use _field_impls::*;
