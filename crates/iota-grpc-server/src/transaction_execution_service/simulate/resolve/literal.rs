// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::NormalizedPackages;
use crate::{RpcError, error::Result};
use iota_types::base_types::{
    ObjectID, STD_ASCII_MODULE_NAME, STD_ASCII_STRUCT_NAME, STD_OPTION_MODULE_NAME,
    STD_OPTION_STRUCT_NAME, STD_UTF8_MODULE_NAME, STD_UTF8_STRUCT_NAME, MOVE_STDLIB_ADDRESS,
};
use iota_sdk2::types::Command;
use move_binary_format::normalized;
use prost_types::value::Kind;
use prost_types::Value;

type Type = normalized::Type<normalized::RcIdentifier>;

pub(super) fn resolve_literal(
    called_packages: &mut NormalizedPackages,
    commands: &[Command],
    arg_idx: usize,
    value: &Value,
) -> Result<Vec<u8>> {
    let literal_type = determine_literal_type(called_packages, commands, arg_idx)?;

    let mut buf = Vec::new();

    resolve_literal_to_type(&mut buf, &literal_type, value)?;

    Ok(buf)
}

fn determine_literal_type(
    called_packages: &mut NormalizedPackages,
    commands: &[Command],
    arg_idx: usize,
) -> Result<Type> {
    fn set_type(maybe_type: &mut Option<Type>, ty: Type) -> Result<()> {
        match maybe_type {
            Some(literal_type) if literal_type == &ty => {}
            Some(_) => {
                return Err(RpcError::new(
                    tonic::Code::InvalidArgument,
                    "unable to resolve literal as it is used as multiple different types across commands",
                ))
            }
            None => {
                *maybe_type = Some(ty);
            }
        }

        Ok(())
    }
    let mut literal_type = None;

    for (command, idx) in super::find_arg_uses(arg_idx, commands) {
        match (command, idx) {
            (Command::MoveCall(move_call), Some(idx)) => {
                let arg_type = super::arg_type_of_move_call_input(called_packages, move_call, idx)?;
                set_type(&mut literal_type, (*arg_type).clone())?;
            }
            (Command::TransferObjects(_), None) => {
                set_type(&mut literal_type, Type::Address)?;
            }

            (Command::SplitCoins(_), Some(_)) => {
                set_type(&mut literal_type, Type::U64)?;
            }
            (Command::MakeMoveVector(make_move_vector), Some(_)) => {
                if let Some(ty) = &make_move_vector.type_ {
                    let ty =
                        iota_types::iota_sdk_types_conversions::type_tag_sdk_to_core(ty.clone())?;
                    let ty = normalized::Type::from_type_tag(&mut called_packages.pool, &ty);
                    set_type(&mut literal_type, ty)?;
                } else {
                    return Err(RpcError::new(
                        tonic::Code::InvalidArgument,
                        "unable to resolve literal as an unknown type",
                    ));
                }
            }

            // Invalid uses of Literal Arguments

            // Pure arg can't be used as an object to transfer
            (Command::TransferObjects(_), Some(_))
            | (Command::Upgrade(_), _)
            | (Command::MergeCoins(_), _)
            | (Command::SplitCoins(_), None) => {
                return Err(RpcError::new(
                    tonic::Code::InvalidArgument,
                    "invalid use of literal",
                ));
            }

            // bug in find_arg_uses
            (Command::MakeMoveVector(_), None)
            | (Command::Publish(_), _)
            | (Command::MoveCall(_), None) => {
                return Err(RpcError::new(
                    tonic::Code::Internal,
                    "error determining type of literal",
                ));
            }
            _ => return Err(RpcError::new(tonic::Code::Internal, "unknown command type")),
        }
    }

    literal_type.ok_or_else(|| {
        RpcError::new(
            tonic::Code::InvalidArgument,
            "unable to determine type of literal",
        )
    })
}

fn resolve_literal_to_type(buf: &mut Vec<u8>, type_: &Type, value: &Value) -> Result<()> {
    match type_ {
        Type::Bool => resolve_as_bool(buf, value),
        Type::U8 => resolve_as_number::<u8>(buf, value),
        Type::U16 => resolve_as_number::<u16>(buf, value),
        Type::U32 => resolve_as_number::<u32>(buf, value),
        Type::U64 => resolve_as_number::<u64>(buf, value),
        Type::U128 => resolve_as_number::<u128>(buf, value),
        Type::U256 => resolve_as_number::<move_core_types::u256::U256>(buf, value),
        Type::Address => resolve_as_address(buf, value),

        // 0x1::ascii::String and 0x1::string::String
        Type::Datatype(dt)
            if dt.module.address == MOVE_STDLIB_ADDRESS
                // 0x1::ascii::String
            && ((dt.module.name.as_ref() == STD_ASCII_MODULE_NAME
                && dt.name.as_ref() == STD_ASCII_STRUCT_NAME)
                // 0x1::string::String
                || (dt.module.name.as_ref() == STD_UTF8_MODULE_NAME
                    && dt.name.as_ref() == STD_UTF8_STRUCT_NAME))
            && dt.type_arguments.is_empty() =>
        {
            resolve_as_string(buf, value)
        }

        // Option<T>
        Type::Datatype(dt)
            if dt.module.address == MOVE_STDLIB_ADDRESS
                && dt.module.name.as_ref() == STD_OPTION_MODULE_NAME
                && dt.name.as_ref() == STD_OPTION_STRUCT_NAME
                && dt.type_arguments.len() == 1 =>
        {
            let ty = dt
                .type_arguments
                .first()
                .expect("length of type_arguments is 1");

            resolve_as_option(buf, ty, value)
        }

        // Vec<T>
        Type::Vector(ty) => resolve_as_vector(buf, ty, value),

        Type::Signer | Type::Datatype(_) | Type::TypeParameter(_) | Type::Reference(_, _) => {
            Err(RpcError::new(
                tonic::Code::InvalidArgument,
                format!("literal cannot be resolved into type {type_}"),
            ))
        }
    }
}

fn resolve_as_bool(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    let b: bool = match &value.kind {
        Some(Kind::BoolValue(b)) => *b,
        Some(Kind::StringValue(s)) => s.parse().map_err(|e| {
            RpcError::new(
                tonic::Code::InvalidArgument,
                format!("literal cannot be resolved as bool: {e}"),
            )
        })?,
        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                "literal cannot be resolved into type bool",
            ))
        }
    };

    bcs::serialize_into(buf, &b)?;

    Ok(())
}

fn resolve_as_number<T>(buf: &mut Vec<u8>, value: &Value) -> Result<()>
where
    T: std::str::FromStr + TryFrom<u64> + serde::Serialize,
    <T as std::str::FromStr>::Err: std::fmt::Display,
    <T as TryFrom<u64>>::Error: std::fmt::Display,
{
    let n: T = match &value.kind {
        Some(Kind::NumberValue(n)) => T::try_from((*n) as u64).map_err(|e| {
            RpcError::new(
                tonic::Code::InvalidArgument,
                format!(
                    "literal cannot be resolved as {}: {e}",
                    std::any::type_name::<T>()
                ),
            )
        })?,

        Some(Kind::StringValue(s)) => s.parse().map_err(|e| {
            RpcError::new(
                tonic::Code::InvalidArgument,
                format!(
                    "literal cannot be resolved as {}: {e}",
                    std::any::type_name::<T>()
                ),
            )
        })?,

        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                format!(
                    "literal cannot be resolved into type {}",
                    std::any::type_name::<T>()
                ),
            ))
        }
    };

    bcs::serialize_into(buf, &n)?;

    Ok(())
}

fn resolve_as_address(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    let address = match &value.kind {
        // parse as ObjectID to handle the case where 0x is present or missing
        Some(Kind::StringValue(s)) => s.parse::<ObjectID>().map_err(|e| {
            RpcError::new(
                tonic::Code::InvalidArgument,
                format!("literal cannot be resolved as address: {e}"),
            )
        })?,
        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                "literal cannot be resolved into type address",
            ))
        }
    };

    bcs::serialize_into(buf, &address)?;

    Ok(())
}

fn resolve_as_string(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    match &value.kind {
        Some(Kind::StringValue(s)) => {
            bcs::serialize_into(buf, s)?;
        }
        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                "literal cannot be resolved into string",
            ))
        }
    };

    Ok(())
}

fn resolve_as_option(buf: &mut Vec<u8>, type_: &Type, value: &Value) -> Result<()> {
    match &value.kind {
        Some(Kind::NullValue(_)) => {
            buf.push(0);
        }
        Some(Kind::BoolValue(_))
        | Some(Kind::NumberValue(_))
        | Some(Kind::StringValue(_))
        | Some(Kind::ListValue(_)) => {
            buf.push(1);
            resolve_literal_to_type(buf, type_, value)?;
        }
        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                "literal cannot be resolved into Option",
            ))
        }
    }

    Ok(())
}

fn resolve_as_vector(buf: &mut Vec<u8>, type_: &Type, value: &Value) -> Result<()> {
    fn write_u32_as_uleb128(buf: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            // Write 7 (lowest) bits of data and set the 8th bit to 1.
            let byte = (value & 0x7f) as u8;
            buf.push(byte | 0x80);
            value >>= 7;
        }
        // Write the remaining bits of data and set the highest bit to 0.
        buf.push(value as u8);
    }

    match &value.kind {
        Some(Kind::ListValue(prost_types::ListValue { values })) => {
            write_u32_as_uleb128(buf, values.len() as u32);
            for value in values {
                resolve_literal_to_type(buf, type_, value)?;
            }
        }
        _ => {
            return Err(RpcError::new(
                tonic::Code::InvalidArgument,
                format!("literal cannot be resolved into type Vector<{type_}>"),
            ));
        }
    }

    Ok(())
}
