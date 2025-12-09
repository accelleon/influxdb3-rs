pub(crate) fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_is_lower = false;
    
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && prev_is_lower {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
            prev_is_lower = false;
        } else {
            result.push(ch);
            prev_is_lower = ch.is_lowercase();
        }
    }
    
    result
}
