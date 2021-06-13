use super::FValue;
use anyhow::Result;
use chrono::{offset::Utc, DateTime};
use serde_json::{Map as JMap, Number as JNumber, Value as JValue};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::SystemTime;

impl From<FValue> for JValue {
    fn from(fvalue: FValue) -> JValue {
        match fvalue {
            FValue::NullValue => JValue::Null,
            FValue::Str(s) => JValue::String(s),
            FValue::Int(i) => JValue::Number(JNumber::from_f64(i as f64).unwrap()),
            FValue::Double(v) => JValue::Number(JNumber::from_f64(v).unwrap()),
            FValue::Bool(b) => JValue::Bool(b),
            FValue::Bytes(bytes) => JValue::Array(
                bytes
                    .into_iter()
                    .map(|each| JValue::Number(JNumber::from_f64(each as f64).unwrap()))
                    .collect(),
            ),
            FValue::Timestamp(dt) => {
                let dt: DateTime<Utc> = dt.into();
                JValue::String(dt.to_rfc3339())
            }
            FValue::Array(vs) => JValue::Array(vs.into_iter().map(JValue::from).collect()),
            FValue::Map(vs) => {
                let m: Vec<(String, JValue)> =
                    vs.into_iter().map(|(k, v)| (k, JValue::from(v))).collect();
                JValue::Object(JMap::from_iter(m))
            }
        }
    }
}

impl From<JValue> for FValue {
    fn from(jvalue: JValue) -> FValue {
        match jvalue {
            JValue::Null => FValue::NullValue,
            JValue::Bool(v) => FValue::Bool(v),
            JValue::Number(n) => {
                if n.is_i64() {
                    FValue::Int(n.as_i64().unwrap())
                } else {
                    FValue::Double(n.as_f64().unwrap())
                }
            }
            JValue::String(s) => match DateTime::parse_from_rfc3339(&s) {
                Ok(dt) => FValue::Timestamp(SystemTime::from(dt)),
                Err(_) => FValue::Str(s),
            },
            JValue::Array(values) => {
                FValue::Array(values.into_iter().map(|v| FValue::from(v)).collect())
            }
            JValue::Object(object_map) => {
                let fvalue_map: HashMap<String, FValue> = object_map
                    .into_iter()
                    .map(|(key, value)| (key, FValue::from(value)))
                    .collect();

                FValue::Map(fvalue_map)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::super::FValue;
    use crate::firestore::value::fvalue::from_fvalue;
    use serde::{Deserialize, Serialize};
    use serde_json;
    use serde_json::{Map as JMap, Number as JNumber, Value as JValue};
    use std::time::SystemTime;

    #[derive(Debug, Serialize, Deserialize)]
    struct Sample {
        f: f64,
        s: String,
        dt: SystemTime,
        arr: Vec<i64>,
    }
    #[test]
    fn json_test() {
        let raw_json = r#"{
            "f": 12.2,
            "s": "something",
            "dt": "2002-10-02T10:00:00-05:00",
            "arr": [1,2,3]
        }"#;

        let jvalue: JValue = serde_json::from_str(raw_json).unwrap();
        let fvalue = FValue::from(jvalue);
        let s: Sample = from_fvalue(fvalue).unwrap();
        assert_eq!("something".to_string(), s.s);
        assert_eq!(12.2f64, s.f);
    }
}
