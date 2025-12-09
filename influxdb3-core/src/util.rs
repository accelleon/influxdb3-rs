pub(crate) fn validate_name(name: &str) -> bool {
    !name.is_empty() &&
        name.bytes().next().map_or(false, |b| b.is_ascii_alphanumeric()) &&
        name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}