use google_cloud_grpc_proto::firestore::v1::{ArrayValue, MapValue};
use google_cloud_grpc_proto::prost_types::Timestamp;
use std::collections::HashMap;

pub use google_cloud_grpc_proto::firestore::v1::{value::ValueType, Document, Value, WriteResult};

#[inline]
pub fn null_value() -> Value {
    Value {
        value_type: Some(ValueType::NullValue(0)),
    }
}

#[inline]
pub fn bool_value(v: bool) -> Value {
    Value {
        value_type: Some(ValueType::BooleanValue(v)),
    }
}

#[inline]
pub fn int_value(i: i64) -> Value {
    Value {
        value_type: Some(ValueType::IntegerValue(i)),
    }
}

#[inline]
pub fn double_value(f: f64) -> Value {
    Value {
        value_type: Some(ValueType::DoubleValue(f)),
    }
}

#[inline]
pub fn timestamp_value<T: Into<Timestamp>>(f: T) -> Value {
    Value {
        value_type: Some(ValueType::TimestampValue(f.into())),
    }
}

#[inline]
pub fn str_value<T: Into<String>>(s: T) -> Value {
    Value {
        value_type: Some(ValueType::StringValue(s.into())),
    }
}

#[inline]
pub fn byte_value(vs: Vec<u8>) -> Value {
    Value {
        value_type: Some(ValueType::BytesValue(vs)),
    }
}

#[inline]
pub fn array_value(s: Vec<Value>) -> Value {
    Value {
        value_type: Some(ValueType::ArrayValue(ArrayValue { values: s })),
    }
}

#[inline]
pub fn map_value(m: HashMap<String, Value>) -> Value {
    Value {
        value_type: Some(ValueType::MapValue(MapValue { fields: m })),
    }
}

#[inline]
pub fn map_value_from_vec(m: Vec<(String, Value)>) -> Value {
    let v: HashMap<String, Value> = m.into_iter().collect();
    map_value(v)
}
