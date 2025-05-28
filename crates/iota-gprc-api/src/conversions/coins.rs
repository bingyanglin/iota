use iota_types::{
    coin::TreasuryCap,
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

    // The fields `balance` and `treasury_balance` in CoinInfoGprc are ambiguous for
    // a general coin type. `total_supply` seems more appropriate from
    // TreasuryCap. For now, `balance` and `treasury_balance` (if it's different
    // from total_supply) are not populated.
    Ok(CoinInfoGprc {
        coin_type_tag: coin_type_tag.to_string(),
        total_supply: total_supply_str,
        balance: None, // Or Some(StringU64 { value: "0".to_string() }) if default needed
        treasury_balance: None, /* This seems redundant if total_supply is present.
                        * If it means something else, its source is unclear. */
    })
}
