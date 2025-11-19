// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    field::FieldMaskTree,
    merge::Merge,
    v0::{
        bcs::BcsData,
        object::Object,
        types::{Digest, ObjectReference},
    },
};

impl Merge<&iota_types::object::Object> for Object {
    fn merge(&mut self, source: &iota_types::object::Object, mask: &FieldMaskTree) {
        if mask.contains("bcs") {
            if let Ok(bcs_bytes) = bcs::to_bytes(source) {
                self.bcs = Some(BcsData {
                    data: bcs_bytes.into(),
                });
            }
        }

        if mask.contains("reference") {
            let mut reference = ObjectReference::default();

            // Check for nested fields within reference
            if let Some(reference_mask) = mask.subtree("reference") {
                if reference_mask.contains("object_id") {
                    reference.object_id = Some(source.id().to_string());
                }

                if reference_mask.contains("version") {
                    reference.version = Some(source.version().value());
                }

                if reference_mask.contains("digest") {
                    reference.digest = Some(Digest {
                        digest: source.digest().inner().to_vec().into(),
                    });
                }
            } else {
                // If no subtree, include all reference fields
                reference.object_id = Some(source.id().to_string());
                reference.version = Some(source.version().value());
                reference.digest = Some(Digest {
                    digest: source.digest().inner().to_vec().into(),
                });
            }

            self.reference = Some(reference);
        }
    }
}
