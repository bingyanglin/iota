// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

/// Trait for types that can provide field presence information
pub(crate) trait FieldPresenceChecker {
    /// Check a field at the current level
    /// Returns: Some((is_present, nested_checker)) if field exists in the
    /// schema          
    ///          None if field doesn't exist in the schema
    /// Field name must be a single field, not a path (no dots)
    fn check_field(&self, field_name: &str) -> Option<(bool, Option<&dyn FieldPresenceChecker>)>;

    /// Check if a field path is present, supporting nested paths like
    /// "reference.object_id"
    fn is_field_present(&self, field_path: &str) -> Option<bool> {
        match field_path.split_once('.') {
            Some((field, rest)) => {
                // Nested path - check this level and recurse
                let (is_present, nested) = self.check_field(field)?;
                if !is_present {
                    return Some(false);
                }
                nested?.is_field_present(rest)
            }
            None => {
                // Single field, no nesting
                let (is_present, _) = self.check_field(field_path)?;
                Some(is_present)
            }
        }
    }
}

/// Macro to automatically implement FieldPresenceChecker for protobuf response
/// types.
///
/// Example: To add support for another protobuf response type, just add:
/// impl_field_presence_checker!(AnotherResponse, {
///     "field1" => field1,
///     "field2" => field2 [nested],
///     // ... other fields
/// });
#[macro_export]
macro_rules! impl_field_presence_checker {
    ($type:ty, {
        $( $tokens:tt )*
    }) => {
        $crate::impl_field_presence_checker!(@parse $type, [], [], [ $( $tokens )* ]);
    };

    // TT muncher: parse nested field
    (@parse $type:ty, [ $( $non_nested_parsed:tt )* ], [ $( $nested_parsed:tt )* ],
     [ $field_name:literal => $field_ident:ident [nested] , $( $rest:tt )* ]) => {
        $crate::impl_field_presence_checker!(@parse $type,
            [ $( $non_nested_parsed )* ],
            [ $( $nested_parsed )* ($field_name, $field_ident), ],
            [ $( $rest )* ]
        );
    };

    // TT muncher: parse nested field (no trailing comma)
    (@parse $type:ty, [ $( $non_nested_parsed:tt )* ], [ $( $nested_parsed:tt )* ],
     [ $field_name:literal => $field_ident:ident [nested] ]) => {
        $crate::impl_field_presence_checker!(@impl $type,
            [ $( $non_nested_parsed )* ],
            [ $( $nested_parsed )* ($field_name, $field_ident), ]
        );
    };

    // TT muncher: parse non-nested field
    (@parse $type:ty, [ $( $non_nested_parsed:tt )* ], [ $( $nested_parsed:tt )* ],
     [ $field_name:literal => $field_ident:ident , $( $rest:tt )* ]) => {
        $crate::impl_field_presence_checker!(@parse $type,
            [ $( $non_nested_parsed )* ($field_name, $field_ident), ],
            [ $( $nested_parsed )* ],
            [ $( $rest )* ]
        );
    };

    // TT muncher: parse non-nested field (no trailing comma)
    (@parse $type:ty, [ $( $non_nested_parsed:tt )* ], [ $( $nested_parsed:tt )* ],
     [ $field_name:literal => $field_ident:ident ]) => {
        $crate::impl_field_presence_checker!(@impl $type,
            [ $( $non_nested_parsed )* ($field_name, $field_ident), ],
            [ $( $nested_parsed )* ]
        );
    };

    // TT muncher: done parsing (empty input)
    (@parse $type:ty, [ $( $non_nested_parsed:tt )* ], [ $( $nested_parsed:tt )* ], []) => {
        $crate::impl_field_presence_checker!(@impl $type,
            [ $( $non_nested_parsed )* ],
            [ $( $nested_parsed )* ]
        );
    };

    // Generate the implementation
    (@impl $type:ty, [ $(  ($non_nested_name:literal, $non_nested_ident:ident), )* ], [ $(  ($nested_name:literal, $nested_ident:ident), )* ]) => {
        impl $crate::utils::FieldPresenceChecker for $type {
            fn check_field(&self, field_name: &str) -> Option<(bool, Option<&dyn $crate::utils::FieldPresenceChecker>)> {
                match field_name {
                    $(
                        $nested_name => {
                            let is_present = self.$nested_ident.is_some();
                            let nested = self.$nested_ident.as_ref().map(|f| f as &dyn $crate::utils::FieldPresenceChecker);
                            Some((is_present, nested))
                        }
                    )*
                    $(
                        $non_nested_name => Some((self.$non_nested_ident.is_some(), None)),
                    )*
                    _ => None,
                }
            }
        }
    };
}

/// Assert field presence for any type implementing MessageFields +
/// FieldPresenceChecker
pub(crate) fn assert_field_presence<T>(response: &T, expected_fields: &[&str], scenario: &str)
where
    T: iota_grpc_types::field::MessageFields + FieldPresenceChecker,
{
    let expected_set: std::collections::HashSet<_> = expected_fields.iter().copied().collect();

    for field in T::FIELDS {
        let field_name = field.name;
        let should_be_present = expected_set.contains(field_name);

        match response.check_field(field_name) {
            Some((is_present, _)) => {
                assert_eq!(
                    is_present, should_be_present,
                    "{field_name} presence mismatch in {scenario}: expected {should_be_present}, got {is_present}",
                );
            }
            None => panic!(
                "Unknown field '{field_name}' in {}, scenario {scenario}",
                std::any::type_name::<T>(),
            ),
        }
    }
}

/// Assert nested field masks on any type implementing MessageFields +
/// FieldPresenceChecker. This function validates that an object contains
/// exactly the fields specified.
/// # Example
/// ```ignore
/// assert_nested_field_masks(
///     &my_object,
///     &["reference.object_id", "reference.version", "bcs"],
///     "test scenario"
/// );
/// ```
pub(crate) fn assert_nested_field_masks<T>(object: &T, field_mask_paths: &[&str], scenario: &str)
where
    T: iota_grpc_types::field::MessageFields + FieldPresenceChecker,
{
    use std::collections::HashSet;

    // Extract top-level field names (everything before first '.', if any)
    let top_level_fields: Vec<&str> = field_mask_paths
        .iter()
        .map(|path| path.split('.').next().unwrap())
        .collect::<HashSet<_>>() // Deduplicate in the same level
        .into_iter()
        .collect();

    // Validate all top-level fields
    assert_field_presence(object, &top_level_fields, scenario);

    // Validate each path depth by depth
    for path in field_mask_paths {
        match object.is_field_present(path) {
            Some(true) => {}
            Some(false) => panic!("Expected field '{path}' in {scenario}, but it was None"),
            None => panic!("Unknown field '{path}' in {scenario}"),
        }
    }
}
