use super::super::FDocument;

use super::error::SerdeError;
use super::FValue;
use std::collections::HashMap;

use std::marker::PhantomData;
use std::time::SystemTime;
use std::vec::IntoIter;

use super::grpc_values::Document;

use serde::{
    de::{DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor},
    forward_to_deserialize_any, Deserializer,
};

pub fn from_document<T>(doc: Document) -> Result<T, SerdeError>
where
    T: DeserializeOwned,
{
    let doc_as_fvalue: FValue = FDocument::from(doc).into();
    from_fvalue(doc_as_fvalue)
}

pub fn from_fvalue<T, F: Into<FValue>>(fvalue: F) -> Result<T, SerdeError>
where
    T: DeserializeOwned,
{
    deserialize_from_with_seed(fvalue.into(), PhantomData)
}

pub fn from_fvalues<T, F: Into<FValue>>(fvalues: Vec<F>) -> Result<Vec<T>, SerdeError>
where
    T: DeserializeOwned,
{
    fvalues
        .into_iter()
        .map(|each| deserialize_from_with_seed(each.into(), PhantomData))
        .collect()
}

fn deserialize_from_with_seed<T, S>(fvalue: FValue, seed: S) -> Result<T, SerdeError>
where
    S: for<'de> DeserializeSeed<'de, Value = T>,
{
    seed.deserialize(FValueDeserializer::from(fvalue))
}

struct FValueDeserializer {
    value: FValue,
}
impl FValueDeserializer {
    fn from(fvalue: FValue) -> FValueDeserializer {
        FValueDeserializer { value: fvalue }
    }
}

impl<'de> Deserializer<'de> for FValueDeserializer {
    type Error = SerdeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            FValue::NullValue => visitor.visit_none(),
            FValue::Str(s) => visitor.visit_string(s),
            FValue::Int(val) => visitor.visit_i64(val),
            FValue::Double(val) => visitor.visit_f64(val),
            FValue::Bool(b) => visitor.visit_bool(b),
            FValue::Bytes(bytes) => visitor.visit_byte_buf(bytes),
            FValue::Array(_) => self.deserialize_seq(visitor),
            FValue::Map(_) => self.deserialize_map(visitor),
            _ => Err(SerdeError::IncompatibleDeserializeType(format!(
                "{:?} not deserialze to struct",
                self.value
            ))),
        }
    }

    /// Hint that the `Deserialize` type is expecting an optional value.
    ///
    /// This allows deserializers that encode an optional value as a nullable
    /// value to convert the null value into `None` and a regular value into
    /// `Some(value)`.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            FValue::NullValue => visitor.visit_none(),
            _ => visitor.visit_some(FValueDeserializer::from(self.value)),
        }
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if name == "SystemTime" {
            if let FValue::Timestamp(system_time) = self.value {
                let system_time_seq_access = SeqFValueAccessForSystemTime::new(system_time);

                visitor.visit_seq(system_time_seq_access)
            } else {
                Err(SerdeError::IncompatibleDeserializeType(format!(
                    "{:?} could not deserialze to system time",
                    self.value
                )))
            }
        } else if let FValue::Map(_) = self.value {
            self.deserialize_map(visitor)
        } else {
            Err(SerdeError::IncompatibleDeserializeType(format!(
                "{:?} could not deserialze to struct",
                self.value
            )))
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let FValue::Map(map_value) = self.value {
            let map_access = MapFValueAccess::new(map_value);
            visitor.visit_map(map_access)
        } else {
            Err(SerdeError::IncompatibleDeserializeType(format!(
                "{:?} could not deserialze to map",
                self.value
            )))
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let FValue::Array(arr) = self.value {
            let seq_access = SeqFValueAccess::new(arr);
            visitor.visit_seq(seq_access)
        } else {
            Err(SerdeError::IncompatibleDeserializeType(format!(
                "{:?} could not deserialze to seq",
                self.value
            )))
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if name == "FValue" {
            Err(SerdeError::IncompatibleDeserializeType(format!(
                "deserializing fvalue to fvalue itself is not implemented yet {:?}",
                variants
            )))
        } else {
            self.deserialize_any(visitor)
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct tuple
        tuple_struct ignored_any identifier
    }
}

struct SeqFValueAccessForSystemTime {
    sec_and_nano_sec: IntoIter<u64>,
}
impl SeqFValueAccessForSystemTime {
    fn new(time: SystemTime) -> Self {
        let unix_time = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let vals = vec![unix_time.as_secs(), unix_time.subsec_nanos() as u64].into_iter();
        Self {
            sec_and_nano_sec: vals,
        }
    }
}
impl<'de> SeqAccess<'de> for SeqFValueAccessForSystemTime {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, SerdeError>
    where
        T: DeserializeSeed<'de>,
    {
        match self.sec_and_nano_sec.next() {
            Some(value) => seed.deserialize(value.into_deserializer()).map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.sec_and_nano_sec.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

/// pass the fValue enum name to the Deserialize visitor created by derive of FValue
struct FValuePrimitiveDeserializer {
    value: FValue,
}

impl<'de> Deserializer<'de> for FValuePrimitiveDeserializer {
    type Error = SerdeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(self.value.as_ref())
    }

    forward_to_deserialize_any! {
        struct map enum seq
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct ignored_any identifier
    }
}

/// TODO(tacogips) deserialize to FValue it self
//struct FValueEnumAccess {
//    fvalue: FValue,
//}
//
//impl<'de> EnumAccess<'de> for FValueEnumAccess {
//    type Error = SerdeError;
//
//    type Variant = FValueVariantAccess;
//
//    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
//    where
//        V: DeserializeSeed<'de>,
//    {
//        let field = seed.deserialize(FValuePrimitiveDeserializer { value: self.fvalue })?;
//
//        unimplemented!()
//    }
//}

/// TODO(tacogips) deserialize to FValue it self
///// pass the fValue enum name to the Deserialize visitor created by derive of FValue
//struct FValueVariantAccess {
//    fvalue: FValue,
//}
//impl<'de> VariantAccess<'de> for FValueVariantAccess {
//    type Error = SerdeError;
//    fn unit_variant(self) -> Result<(), Self::Error> {
//        if self.fvalue == FValue::NullValue {
//            Ok(())
//        } else {
//            Err(SerdeError::InvalidFValueVariable(format!(
//                "could not extract unit variable from {:?}",
//                self.fvalue
//            )))
//        }
//    }
//
//    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
//    where
//        T: DeserializeSeed<'de>,
//    {
//        //fn deserialize_from_with_seed<T, S>(fvalue: FValue, seed: S) -> Result<T, SerdeError>
//        //where
//        //    S: for<'de> DeserializeSeed<'de, Value = T>,
//
//        match self.fvalue {
//            FValue::Str(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Int(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Double(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Bool(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Bytes(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Timestamp(v) => seed.deserialize(v.into_deserializer()),
//            FValue::Array(_) => deserialize_from_with_seed(self.fvalue, seed),
//            FValue::Map(_) => deserialize_from_with_seed(self.fvalue, seed),
//            _ => unimplemented!(),
//        }
//    }
//
//    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
//    where
//        V: Visitor<'de>,
//    {
//        Err(SerdeError::InvalidFValueVariable(
//            "could not extract tuple value from any fvalue".to_owned(),
//        ))
//    }
//
//    fn struct_variant<V>(
//        self,
//        fields: &'static [&'static str],
//        visitor: V,
//    ) -> Result<V::Value, Self::Error>
//    where
//        V: Visitor<'de>,
//    {
//        Err(SerdeError::InvalidFValueVariable(format!(
//            "could not extract tuple value from any fvalue {:?}",
//            fields
//        )))
//    }
//}

struct SeqFValueAccess {
    value_iters: IntoIter<FValue>,
}

impl SeqFValueAccess {
    fn new(values: Vec<FValue>) -> Self {
        Self {
            value_iters: values.into_iter(),
        }
    }
}

impl<'de> SeqAccess<'de> for SeqFValueAccess {
    type Error = SerdeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, SerdeError>
    where
        T: DeserializeSeed<'de>,
    {
        match self.value_iters.next() {
            Some(value) => seed.deserialize(FValueDeserializer::from(value)).map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.value_iters.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

struct MapFValueAccess {
    map_iter: <HashMap<String, FValue> as IntoIterator>::IntoIter,
    current_value: Option<FValue>,
}

impl MapFValueAccess {
    fn new(values: HashMap<String, FValue>) -> Self {
        Self {
            map_iter: values.into_iter(),
            current_value: None,
        }
    }
}

impl<'de> MapAccess<'de> for MapFValueAccess {
    type Error = SerdeError;

    fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, SerdeError>
    where
        T: DeserializeSeed<'de>,
    {
        match self.map_iter.next() {
            Some((key, value)) => {
                self.current_value = Some(value);

                seed.deserialize(key.into_deserializer()).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value, SerdeError>
    where
        T: DeserializeSeed<'de>,
    {
        match self.current_value.take() {
            Some(value) => seed.deserialize(FValueDeserializer::from(value)),
            None => panic!("this panic will be never happend. current value is "),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        match self.map_iter.size_hint() {
            (lower, Some(upper)) if lower == upper => Some(upper),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {

    use super::{from_fvalue, FValue};
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::time::SystemTime;

    #[derive(Deserialize, Debug, PartialEq)]
    struct Testing2 {
        the_field: i64,
    }

    #[derive(Deserialize, Debug)]
    struct Testing {
        something: i64,
        sss: String,
        ttt: SystemTime,
        to_be_none: Option<SystemTime>,
        to_be_some: Option<SystemTime>,
        arr: Vec<f64>,
        another: Testing2,
        option_value: Option<f64>,
        // TODO(tacogips )deserialize to fvalue
        // fvalue: FValue,
    }

    #[test]
    fn deserialize_struct() {
        let time = SystemTime::now();
        let mut input = HashMap::<String, FValue>::new();
        input.insert("something".to_owned(), FValue::from(100i64));
        input.insert("sss".to_owned(), FValue::from("hello".to_owned()));
        input.insert("arr".to_owned(), FValue::from(vec![123.4f64, 555f64]));
        input.insert("ttt".to_owned(), FValue::from(time));
        input.insert("to_be_some".to_owned(), FValue::from(Some(time)));
        input.insert("fvalue".to_owned(), FValue::from(9999f64)); // TODO(tacogips )deserialize to fvalue
        input.insert("option_value".to_owned(), FValue::from(Some(9999f64))); // TODO(tacogips )deserialize to fvalue

        let mut another = HashMap::<String, FValue>::new();
        another.insert("the_field".to_owned(), FValue::from(200i64));
        input.insert("another".to_owned(), FValue::from(another));

        let input = FValue::from(input);
        let actual: Testing = from_fvalue(input).unwrap();

        assert_eq!(100i64, actual.something);
        assert_eq!("hello".to_owned(), actual.sss);
        assert_eq!(Some(time), actual.to_be_some);
        assert_eq!(time, actual.ttt);
        assert_eq!(None, actual.to_be_none);
        assert_eq!(vec![123.4f64, 555f64], actual.arr);
        assert_eq!(Testing2 { the_field: 200 }, actual.another);
        assert_eq!(Some(9999f64), actual.option_value);
    }
}
