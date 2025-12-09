use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::InfluxDBError;
use crate::options::TimestampPrecision;
use crate::{Decode, Encode, PointValue};
use crate::util::validate_name;
use crate::tag_name::{TagMap, TagName};

#[derive(Debug, Default, Clone)]
pub struct Point {
    pub measurement_name: String,
    pub tags: TagMap,
    pub fields: HashMap<String, PointValue>,
    pub time: DateTime<Utc>,
}

impl Point {
    pub fn new_with_measurement(measurement_name: &str) -> Self {
        Self {
            measurement_name: measurement_name.to_string(),
            tags: HashMap::new(),
            fields: HashMap::new(),
            time: Utc::now(),
        }
    }

    pub fn new<I1, I2, K, T, Tz>(measurement_name: &str, tags: I1, fields: I2, timestamp: DateTime<Tz>) -> Self
    where
        I1: IntoIterator<Item = (K, String)>,
        I2: IntoIterator<Item = (String, T)>,
        K: Into<TagName>,
        T: Into<PointValue>,
        Tz: chrono::TimeZone,
    {
        let tags = tags.into_iter().map(|(k, v)| (k.into(), v)).collect();
        let fields = fields.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            measurement_name: measurement_name.to_string(),
            tags,
            fields,
            time: timestamp.to_utc(),
        }
    }

    pub fn get_measurement(&self) -> &str {
        &self.measurement_name
    }

    pub fn set_measurement(&mut self, name: &str) -> &mut Self {
        self.measurement_name = name.to_string();
        self
    }

    pub fn set_timestamp<T>(&mut self, timestamp: DateTime<T>) -> &mut Self
    where
        T: chrono::TimeZone,
    {
        self.time = timestamp.to_utc();
        self
    }

    pub fn set_tag<K>(&mut self, key: K, value: &str) -> &mut Self
    where
        K: TryInto<TagName>,
        <K as TryInto<TagName>>::Error: std::fmt::Debug,
    {
        self.tags.insert(key.try_into().expect("Invalid tag name"), value.to_string());
        self
    }

    pub fn get_tag<K>(&self, key: K) -> Option<&String>
    where
        K: TryInto<TagName>,
        <K as TryInto<TagName>>::Error: std::fmt::Debug,
    {
        self.tags.get(&key.try_into().expect("Invalid tag name"))
    }

    pub fn has_tag<K>(&self, key: K) -> bool
    where
        K: TryInto<TagName>,
        <K as TryInto<TagName>>::Error: std::fmt::Debug,
    {
        self.tags.contains_key(&key.try_into().expect("Invalid tag name"))
    }

    pub fn remove_tag<K>(&mut self, key: K) -> &mut Self
    where
        K: TryInto<TagName>,
        <K as TryInto<TagName>>::Error: std::fmt::Debug,
    {
        self.tags.remove(&key.try_into().expect("Invalid tag name"));
        self
    }

    pub fn get_tag_names(&self) -> Vec<TagName> {
        self.tags.keys().cloned().collect()
    }

    pub fn get_field<'a, T>(&'a self, key: &str) -> Result<Option<T>, InfluxDBError>
    where
        T: Decode<'a>,
    {
        self.fields.get(key).map(|value| T::decode(value)).transpose()
    }

    pub fn set_field<T>(&mut self, key: &str, value: T) -> &mut Self
    where
        T: Encode,
    {
        if validate_name(key) {
            self.fields.insert(key.to_string(), value.encode());
        }
        self
    }

    pub fn remove_field(&mut self, key: &str) -> &mut Self {
        self.fields.remove(key);
        self
    }

    pub fn get_field_names(&self) -> Vec<&String> {
        self.fields.keys().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub(crate) fn serialize(&self, buf: &mut Vec<u8>, precision: TimestampPrecision, default_tags: &TagMap) {
        // <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
        buf.extend(self.measurement_name.as_bytes());
        for (tag_key, tag_value) in default_tags {
            buf.push(b',');
            buf.extend(tag_key.as_bytes());
            buf.push(b'=');
            buf.extend(tag_value.as_bytes());
        }
        for (tag_key, tag_value) in &self.tags {
            buf.push(b',');
            buf.extend(tag_key.as_ref().as_bytes());
            buf.push(b'=');
            buf.extend(tag_value.as_bytes());
        }
        buf.push(b' ');
        let mut first_field = true;
        for (field_key, field_value) in &self.fields {
            if !first_field {
                buf.push(b',');
            }
            first_field = false;
            buf.extend(field_key.as_bytes());
            buf.push(b'=');
            buf.extend(field_value.serialize().as_bytes());
        }
        buf.push(b' ');
        buf.extend(precision.process_timestamp(self.time).to_string().as_bytes());
        buf.push(b'\n');
    }
}

pub trait FromPoint {
    fn from_point(point: Point) -> Result<Self, InfluxDBError>
    where
        Self: Sized;
}

pub trait ToPoint {
    fn to_point(self) -> Point;
}

impl ToPoint for Point {
    fn to_point(self) -> Point {
        self
    }
}