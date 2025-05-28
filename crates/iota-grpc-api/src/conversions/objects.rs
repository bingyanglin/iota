// Placeholder for Object conversion functions

use iota_types::{
    base_types::ObjectID,
    object::{Data, Object, Owner},
};

use crate::{error::GrpcApiError, proto::iota::gprc::v1::ObjectGprc};

pub fn convert_object_to_gprc(
    object_id: &ObjectID,
    object: &Object,
) -> Result<ObjectGprc, GrpcApiError> {
    let object_inner = object.as_inner();

    let version = match &object_inner.data {
        Data::Move(move_obj) => move_obj.version().to_string(),
        Data::Package(package) => package.version().to_string(), /* Assuming MovePackage has a
                                                                  * version() method */
    };

    let owner_type_str = match object_inner.owner {
        Owner::AddressOwner(_) => "AddressOwner".to_string(),
        Owner::ObjectOwner(_) => "ObjectOwner".to_string(),
        Owner::Shared { .. } => "Shared".to_string(),
        Owner::Immutable => "Immutable".to_string(),
    };

    let (data_type_str, raw_data_bytes) = match &object_inner.data {
        Data::Move(move_obj) => (
            move_obj.type_().to_string(), // This is StructTag.to_string()
            move_obj.contents().to_vec(),
        ),
        Data::Package(package) => (
            "MovePackage".to_string(),
            bcs::to_bytes(package).map_err(|e| GrpcApiError::SerializationError(e.to_string()))?,
        ),
    };

    Ok(ObjectGprc {
        object_id: object_id.to_hex_literal(), // ObjectID to hex string
        version,
        owner_type: owner_type_str,
        data_type: data_type_str,
        raw_object: raw_data_bytes,
    })
}

// use crate::proto::iota::gprc::v1::ObjectGprc;
// use iota_types::object::Object as CoreObject;
// use crate::error::GrpcApiError;

// pub fn convert_core_object_to_gprc(
// core_object: &CoreObject,
// ) -> Result<ObjectGprc, GrpcApiError> {
// ... conversion logic ...
// unimplemented!("convert_core_object_to_gprc")
// }
