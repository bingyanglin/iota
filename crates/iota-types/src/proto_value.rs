// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use move_core_types::{
    account_address::AccountAddress, annotated_value as A, annotated_visitor as AV,
    language_storage::TypeTag, u256::U256,
};
use prost_types::{Struct, Value, value::Kind};

use crate::{
    base_types::{RESOLVED_STD_OPTION, move_ascii_str_layout, move_utf8_str_layout},
    id::{ID, UID},
};

/// This is the maximum depth of a proto message
/// The maximum depth of a proto message is 100. Given this value may be nested
/// itself somewhere we'll conservatively cap this to ~80% of that.
const MAX_DEPTH: usize = 80;

pub struct ProtoVisitorBuilder {
    /// Budget to spend on visiting.
    bound: usize,

    /// Current level of nesting depth while visiting.
    depth: usize,
}

struct ProtoVisitor<'a> {
    /// Budget left to spend on visiting.
    bound: &'a mut usize,

    /// Current level of nesting depth while visiting.
    depth: &'a mut usize,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Visitor(#[from] AV::Error),

    #[error("Deserialized value too large")]
    OutOfBudget,

    #[error("Exceeded maximum depth")]
    TooNested,

    #[error("Unexpected type")]
    UnexpectedType,
}

impl ProtoVisitorBuilder {
    pub fn new(bound: usize) -> Self {
        Self { bound, depth: 0 }
    }

    fn new_visitor(&mut self) -> Result<ProtoVisitor<'_>, Error> {
        ProtoVisitor::new(&mut self.bound, &mut self.depth)
    }

    /// Deserialize `bytes` as a `MoveValue` with layout `layout`. Can fail if
    /// the bytes do not represent a value with this layout, or if the
    /// deserialized value exceeds the field/type size budget.
    pub fn deserialize_value(
        mut self,
        bytes: &[u8],
        layout: &A::MoveTypeLayout,
    ) -> Result<Value, Error> {
        let mut visitor = self.new_visitor()?;
        A::MoveValue::visit_deserialize(bytes, layout, &mut visitor)
            .map_err(|_| Error::UnexpectedType)
    }
}

impl Drop for ProtoVisitor<'_> {
    fn drop(&mut self) {
        self.dec_depth();
    }
}

impl<'a> ProtoVisitor<'a> {
    fn new(bound: &'a mut usize, depth: &'a mut usize) -> Result<Self, Error> {
        // Increment the depth since we're creating a new Visitor instance
        Self::inc_depth(depth)?;
        Ok(Self { bound, depth })
    }

    fn inc_depth(depth: &mut usize) -> Result<(), Error> {
        if *depth > MAX_DEPTH {
            Err(Error::TooNested)
        } else {
            *depth += 1;
            Ok(())
        }
    }

    fn dec_depth(&mut self) {
        if *self.depth == 0 {
            panic!("BUG: logic bug in Visitor implementation");
        } else {
            *self.depth -= 1;
        }
    }

    /// Deduct `size` from the overall budget. Errors if `size` exceeds the
    /// current budget.
    fn debit(&mut self, size: usize) -> Result<(), Error> {
        if *self.bound < size {
            Err(Error::OutOfBudget)
        } else {
            *self.bound -= size;
            Ok(())
        }
    }

    fn debit_value(&mut self) -> Result<(), Error> {
        self.debit(size_of::<Value>())
    }

    fn debit_string_value(&mut self, s: &str) -> Result<(), Error> {
        self.debit_str(s)?;
        self.debit_value()
    }

    fn debit_str(&mut self, s: &str) -> Result<(), Error> {
        self.debit(s.len())
    }
}

impl<'b, 'l> AV::Visitor<'b, 'l> for ProtoVisitor<'_> {
    type Value = Value;
    type Error = Error;

    fn visit_u8(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: u8) -> Result<Value, Error> {
        self.debit_value()?;
        Ok(Value::from(value))
    }

    fn visit_u16(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: u16) -> Result<Value, Error> {
        self.debit_value()?;
        Ok(Value::from(value))
    }

    fn visit_u32(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: u32) -> Result<Value, Error> {
        self.debit_value()?;
        Ok(Value::from(value))
    }

    fn visit_u64(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: u64) -> Result<Value, Error> {
        let value = value.to_string();
        self.debit_string_value(&value)?;
        Ok(Value::from(value))
    }

    fn visit_u128(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: u128) -> Result<Value, Error> {
        let value = value.to_string();
        self.debit_string_value(&value)?;
        Ok(Value::from(value))
    }

    fn visit_u256(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: U256) -> Result<Value, Error> {
        let value = value.to_string();
        self.debit_string_value(&value)?;
        Ok(Value::from(value))
    }

    fn visit_bool(&mut self, _: &AV::ValueDriver<'_, 'b, 'l>, value: bool) -> Result<Value, Error> {
        self.debit_value()?;
        Ok(Value::from(value))
    }

    fn visit_address(
        &mut self,
        _: &AV::ValueDriver<'_, 'b, 'l>,
        value: AccountAddress,
    ) -> Result<Value, Error> {
        let value = value.to_canonical_string(true);
        self.debit_string_value(&value)?;
        Ok(Value::from(value))
    }

    fn visit_signer(
        &mut self,
        _: &AV::ValueDriver<'_, 'b, 'l>,
        value: AccountAddress,
    ) -> Result<Value, Error> {
        let value = value.to_canonical_string(true);
        self.debit_string_value(&value)?;
        Ok(Value::from(value))
    }

    fn visit_vector(&mut self, driver: &mut AV::VecDriver<'_, 'b, 'l>) -> Result<Value, Error> {
        let value = if driver.element_layout().is_type(&TypeTag::U8) {
            // Base64 encode arbitrary bytes
            use base64::{Engine, engine::general_purpose::STANDARD};

            if let Some(bytes) = driver
                .bytes()
                .get(driver.position()..(driver.position() + driver.len() as usize))
            {
                let b64 = STANDARD.encode(bytes);
                self.debit_string_value(&b64)?;
                Value::from(b64)
            } else {
                return Err(AV::Error::UnexpectedEof.into());
            }
        } else {
            let mut elems = vec![];
            self.debit_value()?;

            while let Some(elem) =
                driver.next_element(&mut ProtoVisitor::new(self.bound, self.depth)?)?
            {
                elems.push(elem);
            }

            Value::from(elems)
        };

        Ok(value)
    }

    fn visit_struct(&mut self, driver: &mut AV::StructDriver<'_, 'b, 'l>) -> Result<Value, Error> {
        let ty = &driver.struct_layout().type_;
        let layout = driver.struct_layout();

        let value = if layout == &move_ascii_str_layout() || layout == &move_utf8_str_layout() {
            // 0x1::ascii::String or 0x1::string::String

            let lo = driver.position();
            driver.skip_field()?;
            let hi = driver.position();

            // HACK: Bypassing the layout to deserialize its bytes as a Rust type.
            let bytes = &driver.bytes()[lo..hi];
            let s: &str = bcs::from_bytes(bytes).map_err(|_| Error::UnexpectedType)?;
            self.debit_string_value(s)?;
            Value::from(s)
        } else if layout == &UID::layout() || layout == &ID::layout() {
            // 0x2::object::UID or 0x2::object::ID

            let lo = driver.position();
            driver.skip_field()?;
            let hi = driver.position();

            // HACK: Bypassing the layout to deserialize its bytes as a Rust type.
            let bytes = &driver.bytes()[lo..hi];
            let id = AccountAddress::from_bytes(bytes)
                .map_err(|_| Error::UnexpectedType)?
                .to_canonical_string(true);

            self.debit_string_value(&id)?;
            Value::from(id)
        } else if (&ty.address, ty.module.as_ref(), ty.name.as_ref()) == RESOLVED_STD_OPTION {
            // 0x1::option::Option
            self.debit_value()?;
            // Simplified: treat option as a struct with one field
            let mut map = Struct::default();
            while let Some((field, elem)) =
                driver.next_field(&mut ProtoVisitor::new(self.bound, self.depth)?)?
            {
                map.fields.insert(field.name.as_str().to_owned(), elem);
            }
            Value::from(Kind::StructValue(map))
        } else {
            // Arbitrary structs
            let mut map = Struct::default();

            self.debit_value()?;
            for field in &driver.struct_layout().fields {
                self.debit_str(field.name.as_str())?;
            }

            while let Some((field, elem)) =
                driver.next_field(&mut ProtoVisitor::new(self.bound, self.depth)?)?
            {
                map.fields.insert(field.name.as_str().to_owned(), elem);
            }
            Value::from(Kind::StructValue(map))
        };
        Ok(value)
    }

    fn visit_variant(
        &mut self,
        driver: &mut AV::VariantDriver<'_, 'b, 'l>,
    ) -> Result<Value, Error> {
        let mut map = Struct::default();
        self.debit_value()?;

        self.debit_str("@variant")?;
        self.debit_string_value(driver.variant_name().as_str())?;

        map.fields
            .insert("@variant".to_owned(), driver.variant_name().as_str().into());

        for field in driver.variant_layout() {
            self.debit_str(field.name.as_str())?;
        }

        while let Some((field, elem)) =
            driver.next_field(&mut ProtoVisitor::new(self.bound, self.depth)?)?
        {
            map.fields.insert(field.name.as_str().to_owned(), elem);
        }

        Ok(Value::from(Kind::StructValue(map)))
    }
}
