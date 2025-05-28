use iota_types::{
    coin::{CoinMetadata, TreasuryCap},
    object::Data as IotaObjectData, // To access the Move variant
    storage::CoinInfo as CoreStorageCoinInfo,
};
use move_core_types::language_storage::StructTag; // Correct import for StructTag

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{CoinInfoGprc, StringU64}, // Removed StringU256
    server::StateReader,                              /* To fetch related objects
                                                       * like TreasuryCap */
};

pub fn convert_storage_coin_info_to_gprc(
    coin_type_tag: &StructTag,
    core_storage_info: &CoreStorageCoinInfo,
    state_reader: &StateReader,
) -> Result<CoinInfoGprc, GrpcApiError> {
    let mut total_supply_str: Option<StringU64> = None;

    if let Some(treasury_object_id) = core_storage_info.treasury_object_id {
        match state_reader.get_object(&treasury_object_id) {
            Ok(Some(treasury_object)) => {
                if let IotaObjectData::Move(move_obj) = &treasury_object.data {
                    if let Ok(treasury_cap) = bcs::from_bytes::<TreasuryCap>(move_obj.contents()) {
                        total_supply_str = Some(StringU64 {
                            value: treasury_cap.total_supply.value.to_string(),
                        });
                    } else {
                        eprintln!(
                            "[CoinConversion] Failed to deserialize TreasuryCap from object {}",
                            treasury_object_id
                        );
                    }
                } else {
                    eprintln!(
                        "[CoinConversion] Treasury object {} is not a Move object or is unexpected variant",
                        treasury_object_id
                    );
                }
            }
            Ok(None) => {
                eprintln!(
                    "[CoinConversion] Treasury object {} not found",
                    treasury_object_id
                );
            }
            Err(e) => {
                eprintln!(
                    "[CoinConversion] Error fetching treasury object {}: {:?}",
                    treasury_object_id, e
                );
            }
        }
    }

    let mut name: Option<String> = None;
    let mut symbol: Option<String> = None;
    let mut decimals: Option<u32> = None;
    let mut description: Option<String> = None;
    let mut icon_url: Option<String> = None;
    let mut metadata_obj_id_str: Option<String> = None;

    if let Some(metadata_object_id) = core_storage_info.coin_metadata_object_id {
        metadata_obj_id_str = Some(metadata_object_id.to_hex_literal());
        match state_reader.get_object(&metadata_object_id) {
            Ok(Some(metadata_object)) => {
                if let IotaObjectData::Move(move_obj) = &metadata_object.data {
                    match bcs::from_bytes::<CoinMetadata>(move_obj.contents()) {
                        Ok(core_metadata) => {
                            name = Some(core_metadata.name);
                            symbol = Some(core_metadata.symbol);
                            decimals = Some(core_metadata.decimals as u32);
                            description = Some(core_metadata.description);
                            icon_url = core_metadata.icon_url;
                        }
                        Err(e) => {
                            eprintln!(
                                "[CoinConversion] Failed to deserialize CoinMetadata from object {}: {}",
                                metadata_object_id, e
                            );
                        }
                    }
                } else {
                    eprintln!(
                        "[CoinConversion] Metadata object {} is not a Move object or is unexpected variant",
                        metadata_object_id
                    );
                }
            }
            Ok(None) => {
                eprintln!(
                    "[CoinConversion] Metadata object {} not found",
                    metadata_object_id
                );
            }
            Err(e) => {
                eprintln!(
                    "[CoinConversion] Error fetching metadata object {}: {:?}",
                    metadata_object_id, e
                );
            }
        }
    }

    // The fields `balance` and `treasury_balance` in CoinInfoGprc are ambiguous for
    // a general coin type. `total_supply` seems more appropriate from
    // TreasuryCap. For now, `balance` and `treasury_balance` (if it's different
    // from total_supply) are not populated.
    Ok(CoinInfoGprc {
        coin_type_tag: coin_type_tag.to_string(),
        total_supply: total_supply_str,
        balance: None, // Or Some(StringU64 { value: "0".to_string() }) if default needed
        name,
        symbol,
        decimals,
        description,
        icon_url,
        metadata_object_id: metadata_obj_id_str,
    })
}
