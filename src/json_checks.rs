//! Rejects JSON documents that repeat an object key at any depth.
//!
//! Typed serde structs already reject a repeated field, but any part parsed into a map or a
//! `serde_json::Value` silently keeps the last value. Every input document is therefore checked
//! with [`reject_duplicate_keys`] before it is parsed into typed structures.

use std::collections::HashSet;
use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

pub fn reject_duplicate_keys(bytes: &[u8]) -> Result<(), serde_json::Error> {
    serde_json::from_slice::<DuplicateKeyCheck>(bytes).map(|_| ())
}

struct DuplicateKeyCheck;

impl<'de> Deserialize<'de> for DuplicateKeyCheck {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(CheckVisitor)
    }
}

struct CheckVisitor;

impl<'de> Visitor<'de> for CheckVisitor {
    type Value = DuplicateKeyCheck;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(DuplicateKeyCheck)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<DuplicateKeyCheck>()?.is_some() {}
        Ok(DuplicateKeyCheck)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut seen = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate key `{key}`")));
            }
            map.next_value::<DuplicateKeyCheck>()?;
        }
        Ok(DuplicateKeyCheck)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_documents_without_repeated_keys() {
        assert!(reject_duplicate_keys(br#"{"a":1,"b":[{"a":2},{"a":3}],"c":{"d":null}}"#).is_ok());
    }

    #[test]
    fn rejects_a_repeated_key_at_the_top_level() {
        let error = reject_duplicate_keys(br#"{"a":1,"a":2}"#).unwrap_err();
        assert!(error.to_string().contains("duplicate key `a`"));
    }

    #[test]
    fn rejects_a_repeated_key_inside_nested_objects_and_arrays() {
        assert!(
            reject_duplicate_keys(br#"{"tasks":[{"params":{"input":"a","input":"b"}}]}"#).is_err()
        );
        assert!(reject_duplicate_keys(br#"{"meta":{"x":{"y":1,"y":2}}}"#).is_err());
    }

    #[test]
    fn typed_serde_structs_also_reject_repeats_but_values_do_not() {
        // This is why the generic check exists: `Value` keeps the last value silently.
        let value: serde_json::Value = serde_json::from_slice(br#"{"a":1,"a":2}"#).unwrap();
        assert_eq!(value["a"], 2);
    }
}
