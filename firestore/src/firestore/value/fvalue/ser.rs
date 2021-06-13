use super::FValue;
use anyhow::Result;

use serde::ser;
use std::collections::HashMap;

use super::error::SerdeError;
use std::time::{Duration, UNIX_EPOCH};

pub fn to_fvalue<T>(elem: T) -> Result<FValue, SerdeError>
where
    T: ser::Serialize,
{
    elem.serialize(FValueSerializer)
}

pub fn to_fvalues<T>(elems: Vec<T>) -> Result<Vec<FValue>, SerdeError>
where
    T: ser::Serialize,
{
    elems
        .into_iter()
        .map(|each| each.serialize(FValueSerializer))
        .collect()
}

pub struct FValueSerializer;
impl ser::Serializer for FValueSerializer {
    type Ok = FValue;
    type Error = SerdeError;

    type SerializeSeq = FValueSerializeSeq;
    type SerializeTuple = FValueSerializeSeq;
    type SerializeTupleStruct = FValueSerializeSeq;
    type SerializeTupleVariant = FValueSerializeSeq;
    type SerializeMap = FValueSerializeMap;
    type SerializeStruct = FValueSerializeMap;
    type SerializeStructVariant = FValueSerializeMap;

    fn serialize_bool(self, v: bool) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v))
    }

    fn serialize_i8(self, v: i8) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_i16(self, v: i16) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_i32(self, v: i32) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_i64(self, v: i64) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v))
    }

    fn serialize_u8(self, v: u8) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_u16(self, v: u16) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_u32(self, v: u32) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_u64(self, v: u64) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as i64))
    }

    fn serialize_f32(self, v: f32) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as f64))
    }

    fn serialize_f64(self, v: f64) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v as f64))
    }

    fn serialize_char(self, v: char) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<FValue, SerdeError> {
        Ok(FValue::from(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<FValue, SerdeError> {
        Ok(FValue::Bytes(v.to_vec()))
    }

    fn serialize_unit(self) -> Result<FValue, SerdeError> {
        Ok(FValue::NullValue)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<FValue, SerdeError> {
        //TODO(tacogips) better to return empty hashmap?
        Ok(FValue::NullValue)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<FValue, SerdeError> {
        Ok(FValue::from(variant.to_string()))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        v: &T,
    ) -> Result<FValue, SerdeError>
    where
        T: ser::Serialize,
    {
        let v = to_fvalue(v)?;
        let mut m = HashMap::<String, FValue>::new();
        m.insert(name.to_owned(), v);
        Ok(FValue::from(m))
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<FValue, SerdeError>
    where
        T: ser::Serialize,
    {
        let v = to_fvalue(value)?;

        let mut val_m = HashMap::<String, FValue>::new();
        val_m.insert(variant.to_owned(), v);

        let mut m = HashMap::<String, FValue>::new();
        m.insert(name.to_owned(), FValue::from(val_m));
        Ok(FValue::from(m))
    }

    fn serialize_none(self) -> Result<FValue, SerdeError> {
        Ok(FValue::NullValue)
    }

    fn serialize_some<V: ?Sized>(self, value: &V) -> Result<FValue, SerdeError>
    where
        V: ser::Serialize,
    {
        to_fvalue(value)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, SerdeError> {
        let data = match len {
            Some(size) => Vec::<FValue>::with_capacity(size),
            None => Vec::<FValue>::new(),
        };

        Ok(FValueSerializeSeq { data })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, SerdeError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, SerdeError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _enm: &'static str,
        _idx: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, SerdeError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, SerdeError> {
        Ok(FValueSerializeMap {
            struct_name: None,
            map_value: HashMap::new(),
            current_key: None,
        })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, SerdeError> {
        Ok(FValueSerializeMap {
            struct_name: Some(name.to_owned()),
            map_value: HashMap::new(),
            current_key: None,
        })
    }

    fn serialize_struct_variant(
        self,
        _enm: &'static str,
        _idx: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, SerdeError> {
        self.serialize_map(Some(len))
    }
}

pub struct FValueSerializeSeq {
    data: Vec<FValue>,
}

impl ser::SerializeSeq for FValueSerializeSeq {
    type Ok = FValue;

    type Error = SerdeError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        self.data.push(to_fvalue(value)?);
        Ok(())
    }

    fn end(self) -> Result<FValue, SerdeError> {
        Ok(FValue::from(self.data))
    }
}

impl ser::SerializeTuple for FValueSerializeSeq {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<FValue, SerdeError> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for FValueSerializeSeq {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<FValue, SerdeError> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleVariant for FValueSerializeSeq {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<FValue, SerdeError> {
        ser::SerializeSeq::end(self)
    }
}

pub struct FValueSerializeMap {
    struct_name: Option<String>,
    map_value: HashMap<String, FValue>,
    current_key: Option<String>,
}

impl ser::SerializeMap for FValueSerializeMap {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        let maybe_str_value = to_fvalue(key)?;
        if let FValue::Str(key) = maybe_str_value {
            self.current_key = Some(key);
            Ok(())
        } else {
            Err(SerdeError::InvalidMapKey(maybe_str_value))
        }
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), SerdeError>
    where
        T: ser::Serialize,
    {
        let value = to_fvalue(value)?;
        match self.current_key.take() {
            Some(key) => self.map_value.insert(key, value),
            None => panic!("no map key found before `{:?}`", value),
        };
        Ok(())
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), SerdeError>
    where
        K: ser::Serialize,
        V: ser::Serialize,
    {
        let maybe_str_value = to_fvalue(key)?;
        if let FValue::Str(key) = maybe_str_value {
            self.map_value.insert(key, to_fvalue(value)?);
            Ok(())
        } else {
            Err(SerdeError::InvalidMapKey(maybe_str_value))
        }
    }

    fn end(self) -> Result<FValue, SerdeError> {
        if let Some(struct_name) = &self.struct_name {
            if struct_name == "SystemTime" {
                let system_time = UNIX_EPOCH
                    + Duration::from_secs(
                        self.map_value
                            .get("secs_since_epoch")
                            .map(|t| *(t.as_int().unwrap_or(&0)) as u64)
                            .unwrap_or(0u64),
                    );
                Ok(FValue::Timestamp(system_time))
            } else {
                Ok(FValue::from(self.map_value))
            }
        } else {
            Ok(FValue::from(self.map_value))
        }
    }
}

impl ser::SerializeStruct for FValueSerializeMap {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_field<V: ?Sized>(&mut self, key: &'static str, value: &V) -> Result<(), SerdeError>
    where
        V: ser::Serialize,
    {
        ser::SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<FValue, SerdeError> {
        ser::SerializeMap::end(self)
    }
}

impl ser::SerializeStructVariant for FValueSerializeMap {
    type Ok = FValue;
    type Error = SerdeError;

    fn serialize_field<V: ?Sized>(&mut self, key: &'static str, value: &V) -> Result<(), SerdeError>
    where
        V: ser::Serialize,
    {
        ser::SerializeMap::serialize_entry(self, key, value)
    }

    fn end(self) -> Result<FValue, SerdeError> {
        ser::SerializeMap::end(self)
    }
}

#[cfg(test)]
mod test {

    use super::super::map_value_from_vec;
    use super::{to_fvalue, FValue};
    use std::collections::HashMap;

    #[test]
    fn ser_test() {
        {
            let actual = to_fvalue("abc".to_owned()).unwrap();
            assert_eq!(FValue::Str("abc".to_owned()), actual);
        }

        {
            let actual = to_fvalue(333.4f64.to_owned()).unwrap();
            assert_eq!(FValue::Double(333.4), actual);
        }

        {
            let input = vec![Some(11i64), None];
            let actual = to_fvalue(input).unwrap();

            let expected = FValue::Array(vec![FValue::Int(11), FValue::NullValue]);
            assert_eq!(expected, actual);
        }

        {
            let mut input = HashMap::<String, i64>::new();
            input.insert("aaa".to_owned(), 100);
            input.insert("bbb".to_owned(), 200);
            let actual = to_fvalue(input).unwrap();

            let expected =
                map_value_from_vec(vec![("aaa".to_owned(), 100i64), ("bbb".to_owned(), 200i64)]);
            assert_eq!(expected, actual);
        }
    }
}
