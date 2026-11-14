use super::FValue;

use serde::de;
use serde::ser;
use std::fmt::{self, Debug, Display, Formatter};

#[derive(Debug)]
pub enum SerdeError {
    InvalidFValueVariable(String),
    IncompatibleDeserializeType(String),
    InvalidMapKey(FValue),
    CustomError(String),
}
impl Display for SerdeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "serde error {:?}", self)
    }
}

impl ser::Error for SerdeError {
    fn custom<T: Display>(msg: T) -> Self {
        SerdeError::CustomError(msg.to_string())
    }
}

impl de::Error for SerdeError {
    fn custom<T: Display>(msg: T) -> Self {
        SerdeError::CustomError(msg.to_string())
    }
}

impl std::error::Error for SerdeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &*self {
            SerdeError::InvalidFValueVariable(_) => None,
            SerdeError::IncompatibleDeserializeType(_) => None,
            SerdeError::InvalidMapKey(_) => None,
            SerdeError::CustomError(_) => None,
        }
    }
}
