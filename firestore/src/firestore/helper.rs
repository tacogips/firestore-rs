use super::request::DocumentWriteOperation;

use super::value::fdoc::doc_path;
use super::value::FFields;
use serde::Serialize;

pub fn new_write_ope_create<T>(
    parent: Option<String>,
    collection_id: String,
    doc_id: String,
    doc: T,
) -> DocumentWriteOperation
where
    T: Serialize,
{
    DocumentWriteOperation::new_create(parent, collection_id, doc_id, FFields::from(doc))
}

pub fn new_write_ope_update<T>(
    parent: Option<String>,
    collection_id: String,
    doc_id: String,
    update_field_mask: Option<Vec<String>>,
    doc: T,
) -> DocumentWriteOperation
where
    T: Into<FFields>,
{
    DocumentWriteOperation::new_update(
        doc_path(parent, collection_id, doc_id),
        doc.into(),
        update_field_mask,
    )
}

pub fn new_write_ope_upsert<T>(
    parent: Option<String>,
    collection_id: String,
    doc_id: String,
    doc: T,
) -> DocumentWriteOperation
where
    T: Into<FFields>,
{
    DocumentWriteOperation::new_upsert(doc_path(parent, collection_id, doc_id), doc.into())
}

pub fn new_write_ope_delete(
    parent: Option<String>,
    collection_id: String,
    doc_id: String,
) -> DocumentWriteOperation {
    DocumentWriteOperation::new_delete(doc_path(parent, collection_id, doc_id))
}
