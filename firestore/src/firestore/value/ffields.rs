use super::fvalue::to_fvalue;
use super::fvalue::FValue;
use super::grpc_values;

use anyhow::{anyhow, Result};
use std::collections::{hash_map, HashMap};
use std::iter::FromIterator;

use serde_json::{Map as JMap, Number as JNumber, Value as JValue};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct FFields {
    fields: HashMap<String, FValue>,
}

impl FFields {
    pub fn new(m: HashMap<String, FValue>) -> Self {
        Self { fields: m }
    }

    pub fn empty() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn add<K: Into<String>, T: Into<FValue>>(&mut self, name: K, v: T) {
        self.fields.insert(name.into(), v.into());
    }

    pub fn get<K: AsRef<str>>(&self, key: K) -> Option<&FValue> {
        self.fields.get(key.as_ref())
    }

    pub fn to_grpc_fields(self) -> HashMap<String, grpc_values::Value> {
        self.fields
            .into_iter()
            .map(|(k, v)| (k, v.to_grpc_value()))
            .collect()
    }

    pub fn from_grpc_doc(d: grpc_values::Document) -> Self {
        let fields: HashMap<String, FValue> = d
            .fields
            .into_iter()
            .map(|(k, v)| (k, FValue::from(v)))
            .collect();
        FFields { fields }
    }

    pub fn from_json(jv: JValue) -> Result<Self> {
        match jv {
            JValue::Object(m) => {
                let fields: HashMap<String, FValue> =
                    m.into_iter().map(|(k, v)| (k, FValue::from(v))).collect();
                Ok(Self { fields })
            }
            _ => Err(anyhow!("giving json value is not a json object")),
        }
    }

    pub fn into_iter(self) -> hash_map::IntoIter<String, FValue> {
        self.fields.into_iter()
    }

    pub fn as_fvalue(self) -> FValue {
        FValue::Map(self.fields)
    }
}

impl Into<FValue> for FFields {
    fn into(self) -> FValue {
        FValue::Map(self.fields)
    }
}

impl Into<HashMap<String, FValue>> for FFields {
    fn into(self) -> HashMap<String, FValue> {
        self.fields
    }
}

impl Into<HashMap<String, grpc_values::Value>> for FFields {
    fn into(self) -> HashMap<String, grpc_values::Value> {
        self.fields
            .into_iter()
            .map(|(k, v)| (k, v.to_grpc_value()))
            .collect()
    }
}

impl<T> From<T> for FFields
where
    T: Serialize,
{
    fn from(from: T) -> FFields {
        if let Ok(FValue::Map(map_data)) = to_fvalue(from) {
            FFields { fields: map_data }
        } else {
            //TODO(tacogips) TryFrom<T> conflict another impl,so using From<T> instead now.
            panic!("not ffield compatible value")
        }
    }
}

impl From<FFields> for JValue {
    fn from(ffields: FFields) -> JValue {
        let m: Vec<(String, JValue)> = ffields
            .fields
            .into_iter()
            .map(|(k, v)| (k, JValue::from(v)))
            .collect();

        JValue::Object(JMap::from_iter(m))
    }
}
