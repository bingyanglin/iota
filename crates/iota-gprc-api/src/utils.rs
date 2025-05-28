use crate::proto::iota::gprc::v1::StringU64;

// Parses an optional StringU64 into a u64, returning a default value if None or
// parse fails.
pub fn parse_optional_string_u64_to_u64(opt_str_u64: Option<&StringU64>, default_val: u64) -> u64 {
    opt_str_u64
        .and_then(|s| s.value.parse().ok())
        .unwrap_or(default_val)
}

// Placeholder for other utility functions as needed.
