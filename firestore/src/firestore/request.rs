use google_cloud_grpc_proto::firestore::v1::{
    batch_get_documents_request, get_document_request, list_documents_request,
    partition_query_request, run_query_request, transaction_options, write::Operation,
    BatchGetDocumentsRequest, BatchWriteRequest, BeginTransactionRequest, CommitRequest,
    CreateDocumentRequest, DeleteDocumentRequest, Document, DocumentMask, GetDocumentRequest,
    ListCollectionIdsRequest, ListDocumentsRequest, PartitionQueryRequest, RollbackRequest,
    RunQueryRequest, StructuredQuery, TransactionOptions, UpdateDocumentRequest, Value, Write,
    WriteRequest,
};
use google_cloud_grpc_proto::prost_types::Timestamp;
use std::collections::HashMap;
use std::time::SystemTime;

fn validate_partial_document_paths(document_paths: &[String]) -> bool {
    document_paths
        .iter()
        .all(|e| validate_partial_document_path(e))
}

fn validate_partial_document_path(document_path: &String) -> bool {
    if document_path.is_empty() {
        panic!("empty document path");
    }

    if !document_path.starts_with("/") {
        panic!("must start with '/': {}", document_path);
    }

    if document_path.contains("/documents") {
        panic!("must not contains 'documents': {}", document_path);
    }
    true
}

fn project_and_default_database(project_id: String) -> String {
    format!("projects/{}/databases/{}", project_id, default_database())
}

fn default_database() -> String {
    "(default)".to_string()
}

fn fmt_document_path<P: AsRef<str>, D: AsRef<str>>(project_id: P, document_path: D) -> String {
    format!(
        "projects/{}/databases/(default)/documents{}",
        project_id.as_ref(),
        document_path.as_ref()
    )
}

pub(super) fn new_collection_ids_request(
    project_id: String,
    document_path: String,
    chunk_size: Option<i32>,
    page_token: String,
) -> ListCollectionIdsRequest {
    ListCollectionIdsRequest {
        parent: fmt_document_path(project_id, document_path),
        page_size: chunk_size.unwrap_or(100),
        page_token,
    }
}

///TODO (tacogips) deal with read time consistency
pub(super) fn new_get_document_request(
    project_id: String,
    document_path: String,
    field_mask: Option<Vec<String>>,
    transaction: Option<Vec<u8>>,
) -> GetDocumentRequest {
    debug_assert!(validate_partial_document_path(&document_path));
    use get_document_request::ConsistencySelector::Transaction;
    GetDocumentRequest {
        name: fmt_document_path(project_id, document_path),
        mask: field_mask.map(|ms| DocumentMask { field_paths: ms }),
        consistency_selector: transaction.map(|id| Transaction(id)),
    }
}

pub(super) fn new_delete_document_request(
    project_id: String,
    document_path: String,
) -> DeleteDocumentRequest {
    debug_assert!(validate_partial_document_path(&document_path));
    DeleteDocumentRequest {
        name: fmt_document_path(project_id.as_str(), document_path),
        current_document: None,
    }
}

///TODO (tacogips) deal with read time consistency
pub(super) fn new_list_document_request(
    project_id: String,
    document_path: String,
    collection_id: String,
    page_token: String,
    order_by: Option<String>,
    chunk_size: Option<i32>,
    field_mask: Option<Vec<String>>,
    transaction: Option<Vec<u8>>,
) -> ListDocumentsRequest {
    use list_documents_request::ConsistencySelector::Transaction;

    ListDocumentsRequest {
        parent: fmt_document_path(project_id, document_path),
        collection_id,
        page_size: chunk_size.unwrap_or(100),
        page_token,
        order_by: order_by.unwrap_or("".to_owned()),
        mask: to_document_mask(field_mask),
        show_missing: false,
        consistency_selector: transaction.map(|id| Transaction(id)),
    }
}

///TODO (tacogips) deal with read time consistency
pub(super) fn new_batch_get_documents_request(
    project_id: String,
    document_paths: Vec<String>,
    field_mask: Option<Vec<String>>,
    transaction: Option<Vec<u8>>,
) -> BatchGetDocumentsRequest {
    use batch_get_documents_request::ConsistencySelector::Transaction;

    debug_assert!(validate_partial_document_paths(&document_paths));

    BatchGetDocumentsRequest {
        database: project_and_default_database(project_id.clone()),
        documents: document_paths
            .iter()
            .map(|each_path| fmt_document_path(project_id.as_str(), each_path))
            .collect(),
        mask: to_document_mask(field_mask),
        consistency_selector: transaction.map(|id| Transaction(id)),
    }
}

pub(super) fn new_update_document_request<T: Into<HashMap<String, Value>>>(
    project_id: String,
    document_path: String,
    values: T,
    update_field_mask: Option<Vec<String>>,
    response_field_mask: Option<Vec<String>>,
) -> UpdateDocumentRequest {
    let name = fmt_document_path(project_id.as_str(), document_path);

    UpdateDocumentRequest {
        document: Some(new_document(name, values)),
        update_mask: to_document_mask(update_field_mask),
        mask: response_field_mask.map(|ms| DocumentMask { field_paths: ms }),
        current_document: None,
    }
}

pub(super) fn new_create_document_request<T: Into<HashMap<String, Value>>>(
    project_id: String,
    parent_path: String,
    collection_id: String,
    document_id: String,
    values: T,
    response_field_mask: Option<Vec<String>>,
) -> CreateDocumentRequest {
    let parent_path = fmt_document_path(project_id, parent_path);
    CreateDocumentRequest {
        parent: parent_path,
        collection_id,
        document_id,
        document: Some(new_document("".to_owned(), values)),
        mask: response_field_mask.map(|ms| DocumentMask { field_paths: ms }),
    }
}

#[derive(Clone, Debug)]
enum WriteOperation {
    Create(HashMap<String, Value>),
    Update(HashMap<String, Value>),
    Delete,
}

fn to_document_mask(mask: Option<Vec<String>>) -> Option<DocumentMask> {
    mask.map(|ms| DocumentMask { field_paths: ms })
}

#[derive(Clone, Debug)]
pub struct DocumentWriteOperation {
    document_path: String,
    operation: WriteOperation,
    update_field_mask: Option<Vec<String>>,
}

impl DocumentWriteOperation {
    pub fn new_create<T: Into<HashMap<String, Value>>>(
        parent_path: Option<String>,
        collection_id: String,
        doc_id: String,
        fields: T,
    ) -> Self {
        DocumentWriteOperation {
            document_path: format!(
                "{}/{}/{}",
                parent_path
                    .map(|path| format!("/{}", path))
                    .unwrap_or("".to_owned()),
                collection_id,
                doc_id
            ),
            operation: WriteOperation::Create(fields.into()),
            update_field_mask: None,
        }
    }

    pub fn new_upsert<T: Into<HashMap<String, Value>>>(document_path: String, fields: T) -> Self {
        debug_assert!(validate_partial_document_path(&document_path));

        DocumentWriteOperation {
            document_path,
            operation: WriteOperation::Update(fields.into()),
            update_field_mask: None,
        }
    }

    pub fn new_update<T: Into<HashMap<String, Value>>>(
        document_path: String,
        fields: T,
        update_field_mask: Option<Vec<String>>,
    ) -> Self {
        debug_assert!(validate_partial_document_path(&document_path));

        DocumentWriteOperation {
            document_path,
            operation: WriteOperation::Update(fields.into()),
            update_field_mask,
        }
    }

    pub fn new_delete(document_path: String) -> Self {
        debug_assert!(validate_partial_document_path(&document_path));

        DocumentWriteOperation {
            document_path,
            operation: WriteOperation::Delete,
            update_field_mask: None,
        }
    }

    fn into_operation_and_mask(self, project_id: String) -> (Operation, Option<DocumentMask>) {
        let full_document_path = fmt_document_path(project_id, self.document_path);
        let operation = match self.operation {
            WriteOperation::Create(values) => {
                Operation::Update(new_document(full_document_path, values))
            }
            WriteOperation::Update(values) => {
                Operation::Update(new_document(full_document_path, values))
            }
            WriteOperation::Delete => Operation::Delete(full_document_path),
        };
        (operation, to_document_mask(self.update_field_mask))
    }
    fn into_write(self, project_id: String) -> Write {
        let (operation, mask) = self.into_operation_and_mask(project_id);

        Write {
            operation: Some(operation),
            update_mask: mask,
            update_transforms: Vec::new(),
            current_document: None,
        }
    }

    fn into_writes(project_id: String, operations: Vec<DocumentWriteOperation>) -> Vec<Write> {
        operations
            .into_iter()
            .map(|each| each.into_write(project_id.clone()))
            .collect()
    }
}

pub(super) fn new_start_stream_write_request(
    project_id: String,
    stream_id: Option<String>,
) -> WriteRequest {
    WriteRequest {
        database: project_and_default_database(project_id),
        writes: Vec::new(),
        labels: HashMap::new(),
        stream_id: stream_id.unwrap_or("".to_owned()),
        stream_token: Vec::new(),
    }
}

pub(super) fn new_finish_stream_write_request(
    project_id: String,
    stream_token: Vec<u8>,
) -> WriteRequest {
    WriteRequest {
        database: project_and_default_database(project_id),
        writes: Vec::new(),
        labels: HashMap::new(),
        stream_id: "".to_owned(),
        stream_token,
    }
}

pub(super) fn new_stream_write_request(
    project_id: String,
    operations: Vec<DocumentWriteOperation>,
    stream_id: String,
    stream_token: Vec<u8>,
) -> WriteRequest {
    WriteRequest {
        database: project_and_default_database(project_id.clone()),
        writes: DocumentWriteOperation::into_writes(project_id, operations),
        labels: HashMap::new(),
        stream_id,
        stream_token,
    }
}

pub(super) fn new_batch_write_request(
    project_id: String,
    operations: Vec<DocumentWriteOperation>,
) -> BatchWriteRequest {
    BatchWriteRequest {
        database: project_and_default_database(project_id.clone()),
        writes: DocumentWriteOperation::into_writes(project_id, operations),
        labels: HashMap::new(),
    }
}

fn new_document<T: Into<HashMap<String, Value>>>(name: String, fields: T) -> Document {
    Document {
        name,
        fields: fields.into(),
        create_time: None,
        update_time: None,
    }
}

pub(super) fn new_query_request(
    project_id: String,
    parent_path: String,
    query: StructuredQuery,
    transaction: Option<Vec<u8>>,
) -> RunQueryRequest {
    use run_query_request::ConsistencySelector::Transaction;
    use run_query_request::QueryType;
    RunQueryRequest {
        parent: fmt_document_path(project_id, parent_path),
        query_type: Some(QueryType::StructuredQuery(query)),
        consistency_selector: transaction.map(|id| Transaction(id)),
    }
}

pub(super) fn new_partition_query_request(
    project_id: String,
    document_path: String,
    query: StructuredQuery,
    max_partition_count: i64,
    chunk_size: i32,
    token: String,
) -> PartitionQueryRequest {
    use partition_query_request::QueryType;
    PartitionQueryRequest {
        parent: fmt_document_path(project_id, document_path),
        query_type: Some(QueryType::StructuredQuery(query)),
        partition_count: max_partition_count,
        page_size: chunk_size,
        page_token: token,
    }
}

///TODO(tacogips) need retry_transaction?
pub(super) fn new_begin_transaction_request(
    project_id: String,
    read_only_time: Option<SystemTime>,
) -> BeginTransactionRequest {
    let option = match read_only_time {
        Some(read_only_time) => TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(
                transaction_options::ReadOnly {
                    consistency_selector: Some(
                        transaction_options::read_only::ConsistencySelector::ReadTime(
                            Timestamp::from(read_only_time),
                        ),
                    ),
                },
            )),
        },

        None => TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite {
                    retry_transaction: Vec::new(),
                },
            )),
        },
    };

    BeginTransactionRequest {
        database: project_and_default_database(project_id),
        options: Some(option),
    }
}

pub(super) fn new_commit_request(
    project_id: String,
    operations: Vec<DocumentWriteOperation>,
    transaction: Option<Vec<u8>>,
) -> CommitRequest {
    CommitRequest {
        database: project_and_default_database(project_id.clone()),
        writes: DocumentWriteOperation::into_writes(project_id, operations),
        transaction: transaction.unwrap_or(Vec::new()),
    }
}

pub(super) fn new_rollback_request(project_id: String, transaction: Vec<u8>) -> RollbackRequest {
    RollbackRequest {
        database: project_and_default_database(project_id),
        transaction,
    }
}
