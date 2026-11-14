use super::grpc_values::{self, ValueType, WriteResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use strum_macros::AsRefStr;

mod de;
mod error;
mod json_conv;
mod ser;

pub use de::{from_document, from_fvalue, from_fvalues};
pub use ser::{to_fvalue, to_fvalues};

//TODO(tacogips) deal with Reference And GeoPoint
#[derive(Debug, PartialEq, Deserialize, Serialize, AsRefStr, Clone)]
pub enum FValue {
    NullValue,
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Timestamp(SystemTime),
    Array(Vec<FValue>),
    Map(HashMap<String, FValue>),
}

/// generate function which turn the enum into Option<{TargetType}>
macro_rules! fvalue_into {
    ($fn_name:ident, $value:ident, $ty:ty) => {
        pub fn $fn_name(self) -> Option<$ty> {
            if let FValue::$value(v) = self {
                Some(v)
            } else {
                None
            }
        }
    };
}

/// generate function which turn the enum into Option<{TargetType}>
macro_rules! fvalue_as {
    ($fn_name:ident, $value:ident, $ty:ty) => {
        pub fn $fn_name(&self) -> Option<&$ty> {
            if let FValue::$value(v) = self {
                Some(v)
            } else {
                None
            }
        }
    };
}

impl FValue {
    fvalue_into!(into_string, Str, String);
    fvalue_into!(into_int, Int, i64);
    fvalue_into!(into_bool, Bool, bool);
    fvalue_into!(into_double, Double, f64);
    fvalue_into!(into_bytes, Bytes, Vec<u8>);
    fvalue_into!(into_system, Timestamp, SystemTime);
    fvalue_into!(into_array, Array, Vec<FValue>);
    fvalue_into!(into_map, Map, HashMap<String, FValue>);

    fvalue_as!(as_string, Str, String);
    fvalue_as!(as_int, Int, i64);
    fvalue_as!(as_bool, Bool, bool);
    fvalue_as!(as_double, Double, f64);
    fvalue_as!(as_bytes, Bytes, Vec<u8>);
    fvalue_as!(as_system, Timestamp, SystemTime);
    fvalue_as!(as_array, Array, Vec<FValue>);
    fvalue_as!(as_map, Map, HashMap<String, FValue>);

    pub fn to_grpc_value(self) -> grpc_values::Value {
        self.to_grpc_value_with_depth(0)
    }

    fn to_grpc_value_with_depth(self, depth: i32) -> grpc_values::Value {
        assert!(depth <= 20, "array or map depth must be less than equal 20");

        match self {
            FValue::NullValue => grpc_values::null_value(),
            FValue::Str(v) => grpc_values::str_value(v),
            FValue::Int(v) => grpc_values::int_value(v),
            FValue::Double(v) => grpc_values::double_value(v),
            FValue::Bool(v) => grpc_values::bool_value(v),
            FValue::Bytes(v) => grpc_values::byte_value(v),
            FValue::Timestamp(v) => grpc_values::timestamp_value(v),
            FValue::Array(vs) => {
                let vs: Vec<grpc_values::Value> = vs
                    .into_iter()
                    .map(|e| e.to_grpc_value_with_depth(depth + 1))
                    .collect();
                grpc_values::array_value(vs)
            }
            FValue::Map(vs) => {
                let vs: HashMap<String, grpc_values::Value> = vs
                    .into_iter()
                    .map(|(k, v)| (k, v.to_grpc_value_with_depth(depth + 1)))
                    .collect();
                grpc_values::map_value(vs)
            }
        }
    }

    pub(crate) fn from_grpc_value(v: grpc_values::Value) -> Self {
        Self::from_grpc_value_with_depth(v, 0)
    }

    fn from_grpc_value_with_depth(v: grpc_values::Value, depth: i64) -> Self {
        match v.value_type {
            Some(ValueType::NullValue(_)) => FValue::NullValue,
            Some(ValueType::BooleanValue(v)) => FValue::Bool(v),
            Some(ValueType::IntegerValue(v)) => FValue::Int(v),
            Some(ValueType::DoubleValue(v)) => FValue::Double(v),
            Some(ValueType::TimestampValue(v)) => FValue::Timestamp(v.into()),
            Some(ValueType::StringValue(v)) => FValue::Str(v),
            Some(ValueType::BytesValue(v)) => FValue::Bytes(v),
            Some(ValueType::ArrayValue(v)) => FValue::Array(
                v.values
                    .into_iter()
                    .map(|v| Self::from_grpc_value_with_depth(v, depth + 1))
                    .collect(),
            ),
            Some(ValueType::MapValue(v)) => FValue::Map(
                v.fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::from_grpc_value_with_depth(v, depth + 1)))
                    .collect(),
            ),

            Some(ValueType::ReferenceValue(_v)) => unimplemented!("reference not supported yet"),
            Some(ValueType::GeoPointValue(_v)) => unimplemented!("geopoint not supported yet"),
            _ => panic!("null value type "),
        }
    }

    pub fn from_write_result(wr: WriteResult) -> Vec<FValue> {
        wr.transform_results
            .into_iter()
            .map(|e| FValue::from(e))
            .collect()
    }

    pub fn from_write_results(wrs: Vec<WriteResult>) -> Vec<Vec<FValue>> {
        wrs.into_iter()
            .map(|each| Self::from_write_result(each))
            .collect()
    }
}

/// generate From<{type}> -> FValue function.
macro_rules! fvalue_from {
    ($ty:ty, $value:ident) => {
        impl From<$ty> for FValue {
            fn from(v: $ty) -> Self {
                FValue::$value(v)
            }
        }
    };
}

fvalue_from!(String, Str);
fvalue_from!(bool, Bool);
fvalue_from!(i64, Int);
fvalue_from!(f64, Double);
fvalue_from!(Vec<u8>, Bytes);
fvalue_from!(SystemTime, Timestamp);

impl From<&str> for FValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_string())
    }
}

impl From<grpc_values::Value> for FValue {
    fn from(v: grpc_values::Value) -> Self {
        Self::from_grpc_value(v)
    }
}

impl<T> From<Vec<T>> for FValue
where
    T: Into<FValue>,
{
    fn from(v: Vec<T>) -> Self {
        let v: Vec<FValue> = v.into_iter().map(|each| each.into()).collect();
        FValue::Array(v)
    }
}

impl<T> From<HashMap<String, T>> for FValue
where
    T: Into<FValue>,
{
    fn from(v: HashMap<String, T>) -> Self {
        let v: HashMap<String, FValue> = v.into_iter().map(|(k, v)| (k, v.into())).collect();
        FValue::Map(v)
    }
}

impl<T> From<Option<T>> for FValue
where
    T: Into<FValue>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(container) => container.into(),
            None => FValue::NullValue,
        }
    }
}

pub fn array_value_from_vec<T: Into<FValue>>(m: Vec<T>) -> FValue {
    let v: Vec<FValue> = m.into_iter().map(|v| v.into()).collect();
    FValue::from(v)
}

pub fn map_value_from_vec<K: Into<String>, T: Into<FValue>>(m: Vec<(K, T)>) -> FValue {
    let v: HashMap<String, FValue> = m.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
    FValue::from(v)
}
