mod client;
mod query;
mod request;
mod value;

mod helper;
pub mod raw;

pub use client::{
    FirestoreClient, MissingDocPaths, TransactionOperation, MAX_BATCH_WRTIE_SIZE, MAX_IN_CLAUS_NUM,
    MAX_WRITE_OPE_IN_TX,
};

pub use query::QueryBuilder;
pub use value::{
    fdoc::{doc_path, FDocument, FDocumentPath},
    ffields::FFields,
    fvalue::{array_value_from_vec, map_value_from_vec, FValue},
    serde::{from_document, from_fvalue, to_fvalue},
};

pub use helper::{
    new_write_ope_create, new_write_ope_delete, new_write_ope_update, new_write_ope_upsert,
};
pub use request::DocumentWriteOperation;

pub mod size_calculator {

    pub const HASH_MAP_ADDITIONAL_BYTES: usize = 32 + 15; //15 is for map name

    //https://firebase.google.com/docs/firestore/storage-size#document-size
    use std::time::SystemTime;
    //https://firebase.google.com/docs/firestore/storage-size#document-size
    //
    pub fn datetime_size(s: &SystemTime) -> usize {
        8
    }

    pub fn datetime_size_opt(v: &Option<SystemTime>) -> usize {
        match v {
            Some(vs) => datetime_size(vs),
            None => 0,
        }
    }

    pub fn usize_size(s: &usize) -> usize {
        8
    }

    pub fn string_size(s: &str) -> usize {
        s.len() * 5
    }

    pub fn number_size(v: &i64) -> usize {
        8
    }

    pub fn float_size(v: &f32) -> usize {
        8
    }

    pub fn string_size_opt(v: &Option<String>) -> usize {
        match v {
            Some(v) => string_size(v),
            None => 0,
        }
    }

    pub fn string_size_vec(vs: &Vec<String>) -> usize {
        vs.iter().map(|s| string_size(s)).sum()
    }

    pub fn string_size_vec_opt(v: &Option<Vec<String>>) -> usize {
        match v {
            Some(vs) => string_size_vec(vs),
            None => 0,
        }
    }
}
