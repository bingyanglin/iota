// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{str::FromStr, sync::Arc};

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    v0::{
        dynamic_field as grpc_dynamic_field,
        state_service::{self as grpc_state, state_service_server::StateService},
    },
};
use iota_types::base_types::ObjectID;
use prost_types::FieldMask;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::types::GrpcStateReader;

/// Default fields to return when no read_mask is specified
/// Includes basic dynamic field info: parent and field_id
pub const READ_MASK_DEFAULT: &str = "parent,field_id";

/// Maximum number of dynamic fields to return in a single response
pub const DEFAULT_DYNAMIC_FIELDS_LIMIT: usize = 50;
pub const MAX_DYNAMIC_FIELDS_LIMIT: usize = 1000;

pub struct StateGrpcService {
    pub state_reader: Arc<dyn GrpcStateReader>,
}

impl StateGrpcService {
    pub fn new(state_reader: Arc<dyn GrpcStateReader>) -> Self {
        Self { state_reader }
    }
}

#[tonic::async_trait]
impl StateService for StateGrpcService {
    type ListDynamicFieldsStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<grpc_state::ListDynamicFieldsResponse, Status>>
                + Send,
        >,
    >;

    async fn list_dynamic_fields(
        &self,
        request: Request<grpc_state::ListDynamicFieldsRequest>,
    ) -> Result<Response<Self::ListDynamicFieldsStream>, Status> {
        let req = request.into_inner();

        // Parse the parent ObjectID
        let parent = req
            .parent
            .ok_or_else(|| Status::invalid_argument("parent is required"))?;

        let parent_object_id = ObjectID::from_hex_literal(&parent)
            .or_else(|_| ObjectID::from_str(&parent))
            .map_err(|e| Status::invalid_argument(format!("Invalid parent ObjectID: {}", e)))?;

        // Parse and validate the read_mask
        let read_mask = {
            let mask = req
                .read_mask
                .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));

            // Validate the field mask paths against the DynamicField message structure
            mask.validate::<grpc_dynamic_field::DynamicField>()
                .map_err(|invalid_path| {
                    Status::invalid_argument(format!(
                        "Invalid field path in read_mask: '{}'",
                        invalid_path
                    ))
                })?;

            FieldMaskTree::from(mask)
        };

        // Check what needs to be fetched based on read_mask
        use grpc_dynamic_field::DynamicField;
        let needs_field_object = read_mask.contains(DynamicField::FIELD_OBJECT_FIELD.name)
            || read_mask.contains(DynamicField::VALUE_FIELD.name);
        let needs_child_object = read_mask.contains(DynamicField::CHILD_OBJECT_FIELD.name);

        // Determine limit for pagination
        let limit = DEFAULT_DYNAMIC_FIELDS_LIMIT;

        debug!(
            "Starting to stream dynamic fields for parent {} with read_mask: {}, needs_field_object: {}, needs_child_object: {}, limit: {}",
            parent_object_id,
            read_mask.to_field_mask().display(),
            needs_field_object,
            needs_child_object,
            limit
        );

        // Clone what we need for the async stream
        let state_reader = Arc::clone(&self.state_reader);

        // Create async stream that handles pagination
        let stream = async_stream::try_stream! {
            let mut cursor: Option<ObjectID> = None;

            loop {
                // Fetch limit + 1 to determine if there are more results
                let dynamic_field_infos_ext = state_reader
                    .get_dynamic_fields(
                        parent_object_id,
                        cursor,
                        limit + 1,
                        needs_field_object,
                        needs_child_object,
                    )
                    .map_err(|e| Status::internal(format!("Failed to fetch dynamic fields: {}", e)))?;

                // Check if there are more results
                let has_next = dynamic_field_infos_ext.len() > limit;

                // Take only up to limit for this batch
                let current_batch: Vec<_> = dynamic_field_infos_ext.into_iter().take(limit).collect();

                // Update cursor to the last field_id for the next iteration
                if let Some(last) = current_batch.last() {
                    cursor = Some(last.info.object_id);
                }

                // Convert DynamicFieldInfoExt to proto DynamicField with field mask filtering
                let dynamic_fields: Vec<grpc_dynamic_field::DynamicField> = current_batch
                    .into_iter()
                    .map(|df_info_ext| build_dynamic_field(df_info_ext, &read_mask))
                    .collect();

                let batch_size = dynamic_fields.len();
                debug!(
                    "Streaming batch of {} dynamic fields for parent {}, has_next: {}",
                    batch_size,
                    parent_object_id,
                    has_next
                );

                // Yield the response
                yield grpc_state::ListDynamicFieldsResponse {
                    dynamic_fields,
                    has_next: Some(has_next),
                };

                // Stop if no more results
                if !has_next {
                    debug!(
                        "Completed streaming dynamic fields for parent {}",
                        parent_object_id
                    );
                    break;
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Build a DynamicField message from DynamicFieldInfoExt, applying field mask
/// filtering Only populates fields that are requested in the read_mask to
/// optimize response size
fn build_dynamic_field(
    df_info_ext: crate::types::DynamicFieldInfoExt,
    read_mask: &FieldMaskTree,
) -> grpc_dynamic_field::DynamicField {
    use grpc_dynamic_field::{DynamicField, dynamic_field::DynamicFieldKind};
    use iota_grpc_types::v0::common::BcsData;
    use iota_types::dynamic_field::DynamicFieldType;

    let mut message = DynamicField::default();
    let df_info = df_info_ext.info;

    // Populate kind if requested
    if read_mask.contains(DynamicField::KIND_FIELD.name) {
        let kind = match df_info.type_ {
            DynamicFieldType::DynamicField => DynamicFieldKind::Field as i32,
            DynamicFieldType::DynamicObject => DynamicFieldKind::Object as i32,
        };
        message.kind = Some(kind);
    }

    // Populate parent if requested
    if read_mask.contains(DynamicField::PARENT_FIELD.name) {
        // Parent is implicitly known from the request context, but we don't
        // store it in DynamicFieldInfo Skip for now as it's handled at
        // request level
    }

    // Populate field_id if requested
    if read_mask.contains(DynamicField::FIELD_ID_FIELD.name) {
        message.field_id = Some(df_info.object_id.to_hex_uncompressed());
    }

    // Populate name if requested
    if read_mask.contains(DynamicField::NAME_FIELD.name) {
        message.name = Some(BcsData {
            data: df_info.bcs_name,
        });
    }

    // Populate value_type if requested
    if read_mask.contains(DynamicField::VALUE_TYPE_FIELD.name) {
        message.value_type = Some(df_info.object_type);
    }

    // Populate child_id if requested and available (only for DynamicObject
    // fields)
    if read_mask.contains(DynamicField::CHILD_ID_FIELD.name) {
        message.child_id = df_info_ext.child_id.map(|id| id.to_hex_uncompressed());
    }

    // Populate field_object if requested
    if read_mask.contains(DynamicField::FIELD_OBJECT_FIELD.name) {
        if let Some(field_obj) = df_info_ext.field_object.as_ref() {
            message.field_object = convert_object_to_grpc(field_obj);
        }
    }

    // Populate value if requested (extract from field_object)
    if read_mask.contains(DynamicField::VALUE_FIELD.name) {
        if let Some(field_obj) = &df_info_ext.field_object {
            message.value = extract_dynamic_field_value(field_obj);
        }
    }

    // Populate child_object if requested
    if read_mask.contains(DynamicField::CHILD_OBJECT_FIELD.name) {
        if let Some(child_obj) = df_info_ext.child_object.as_ref() {
            message.child_object = convert_object_to_grpc(child_obj);
        }
    }

    message
}

/// Convert iota_types::object::Object to gRPC Object
fn convert_object_to_grpc(
    object: &iota_types::object::Object,
) -> Option<iota_grpc_types::v0::object::Object> {
    use iota_grpc_types::v0::{common::BcsData, object::Object as GrpcObject};

    // Serialize the object to BCS
    let bcs_bytes = bcs::to_bytes(object).ok()?;

    Some(GrpcObject {
        bcs: Some(BcsData { data: bcs_bytes }),
        digest: Some(object.digest().to_string()),
        object_id: Some(object.id().to_hex_uncompressed()),
        version: Some(object.version().value()),
    })
}

/// Extract the value from a Field<K, V> dynamic field object
fn extract_dynamic_field_value(
    field_object: &iota_types::object::Object,
) -> Option<iota_grpc_types::v0::common::BcsData> {
    use iota_grpc_types::v0::common::BcsData;

    // Get the Move object data
    let move_obj = field_object.data.try_as_move()?;

    // The dynamic field structure is Field<Name, Value>
    // We need to extract the "value" field from the struct
    // The Move object has its contents which we can access
    let struct_contents = move_obj.contents();

    // For dynamic fields, the value is stored as BCS bytes
    // We serialize the entire contents as BCS since the field structure
    // contains both name and value
    bcs::to_bytes(struct_contents)
        .ok()
        .map(|data| BcsData { data })
}
