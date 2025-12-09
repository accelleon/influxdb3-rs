use std::collections::HashMap;
use std::ops::Deref;

use crate::util::validate_name;
use crate::InfluxDBError;

pub type TagMap = HashMap<TagName, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagName(String);

impl TryFrom<&str> for TagName {
    type Error = InfluxDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if !validate_name(value) {
            Err(InfluxDBError::InvalidTagName(value.to_string()))
        } else {
            Ok(TagName(value.to_string()))
        }
    }
}

impl TryFrom<String> for TagName {
    type Error = InfluxDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !validate_name(&value) {
            Err(InfluxDBError::InvalidTagName(value.to_string()))
        } else {
            Ok(TagName(value))
        }
    }
}

impl TryFrom<&String> for TagName {
    type Error = InfluxDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        if !validate_name(value) {
            Err(InfluxDBError::InvalidTagName(value.to_string()))
        } else {
            Ok(TagName(value.to_string()))
        }
    }
}

impl AsRef<str> for TagName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&TagName> for TagName {
    fn from(value: &TagName) -> Self {
        TagName(value.0.clone())
    }
}

impl Deref for TagName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}