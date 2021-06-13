use super::query::QueryBuilder;
use super::request;
use crate::fetch_through_all_tokens;
use crate::grpc::{
    auth::{auth_interceptor, scopes, TokenManager, TokenManagerBuilder},
    connection_point,
    error::GrpcErrorStatus,
    GrpcChannel,
};

use crate::firestore::{
    value::{array_value_from_vec, doc_path, map_value_from_vec, FFields, FValue},
    FDocument,
};

use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};

use anyhow::{anyhow, Error, Result};
use futures::{Future, FutureExt, Stream};

use batch_get_documents_response::Result as DocResult;
use google_cloud_grpc_proto::{
    firestore::v1::{
        batch_get_documents_response, firestore_client, Cursor, Document, StructuredQuery, Value,
        WriteResult,
    },
    tonic::{transport::Channel, Code},
};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use yup_oauth2::authenticator::{DefaultHyperClient, HyperClientBuilder};

//TODO 413 Entity too large might occure if set to 500
//pub const MAX_BATCH_WRTIE_SIZE: usize = 500;
pub const MAX_BATCH_WRTIE_SIZE: usize = 450;

pub const MAX_IN_CLAUS_NUM: usize = 10;
pub const MAX_BATCH_GET_DOC_NUM: usize = 1000; //TODO(tacogips) confirm

// failed :Status { code: InvalidArgument, message: "datastore transaction or write too big.", metadata: MetadataMap { headers: {"content-type": "application/grpc", "date": "Wed, 12 May 2021 15:59:53 GMT", "alt-svc": "h3-29=\":443\"; ma=2592000,h3-T051=\":443\"; ma=2592000,h3-Q050=\":443\"; ma=2592000,h3-Q046=\":443\"; ma=2592000,h3-Q043=\":443\"; ma=2592000,quic=\":443\"; ma=2592000; v=\"46,43\""} } }
//pub const MAX_WRITE_OPE_IN_TX: usize = 500;
//pub const MAX_WRITE_OPE_IN_TX: usize = 200;
pub const MAX_WRITE_OPE_IN_TX: usize = 500;

pub type MissingDocPaths = Vec<String>;

pub struct TransactionOperation {
    pub transaction: Vec<u8>,
    operations: Vec<request::DocumentWriteOperation>,
}

impl TransactionOperation {
    fn new(transaction: Vec<u8>) -> TransactionOperation {
        TransactionOperation {
            transaction,
            operations: Vec::<request::DocumentWriteOperation>::new(),
        }
    }
    pub fn add_operation(&mut self, write_operation: request::DocumentWriteOperation) {
        self.operations.push(write_operation)
    }
}

/// this trait is for hacking async closure lifetime issue(?)
///
/// https://www.reddit.com/r/rust/comments/hey4oa/help_lifetimes_on_async_functions_with_callbacks/
/// https://github.com/rustasync/team/issues/19
/// https://gendignoux.com/blog/2020/12/17/rust-async-type-system-limits.html
pub trait WithTransaction<'a, Res, Ctx> {
    type Output: 'a + Future<Output = Result<Res>>;
    fn call(
        &self,
        arg: &'a mut FirestoreClient,
        tx: &'a mut TransactionOperation,
        context: Ctx,
    ) -> Self::Output;
}

impl<'a, R, F, Res, Ctx> WithTransaction<'a, Res, Ctx> for F
where
    R: 'a,
    F: Fn(&'a mut FirestoreClient, &'a mut TransactionOperation, Ctx) -> R,
    R: Future<Output = Result<Res>> + 'a,
{
    type Output = R;
    fn call(
        &self,
        arg: &'a mut FirestoreClient,
        tx: &'a mut TransactionOperation,
        context: Ctx,
    ) -> R {
        self(arg, tx, context)
    }
}

pub struct FirestoreClient {
    project_id: String,
    firestore_client: firestore_client::FirestoreClient<Channel>,
    token_manager: Arc<TokenManager<<DefaultHyperClient as HyperClientBuilder>::Connector>>,
}

pub(crate) fn id_filter<T>() -> impl FnMut(&T) -> bool + Copy {
    |_: &T| true
}

impl FirestoreClient {
    pub async fn with_service_account_file(
        project_id: String,
        service_acocunt_cred_path: PathBuf,
    ) -> Result<FirestoreClient> {
        let channel = GrpcChannel::new_connected_channnel(&connection_point::FIRESTORE).await?;

        let token_manager =
            TokenManagerBuilder::new(vec![&scopes::CLOUD_PLATFORM, &scopes::DATASTORE])
                .service_account_file(service_acocunt_cred_path)
                .build()
                .await?;

        let token_manager = Arc::new(token_manager);
        let shared_token = token_manager.shared_token();

        let firestore_client = firestore_client::FirestoreClient::with_interceptor(
            channel.opened_channel.unwrap(),
            auth_interceptor(shared_token),
        );
        Ok(Self {
            project_id,
            firestore_client,
            token_manager,
        })
    }
    pub fn refresh_auth_token(&self) -> Result<()> {
        self.token_manager.force_refresh_token()
    }

    /// attention : with_tx:F sould  be a function pointer, but closuere.
    pub async fn in_transaction<F, R, Ctx>(&mut self, ctx: Ctx, with_tx: F) -> Result<R>
    where
        F: for<'a> WithTransaction<'a, R, Ctx>,
    {
        let tx = self
            .firestore_client
            .begin_transaction(request::new_begin_transaction_request(
                self.project_id.clone(),
                None,
            ))
            .await?
            .into_inner()
            .transaction;

        let mut tx_ope = TransactionOperation::new(tx);
        let maybe_panic_in_tx = AssertUnwindSafe(with_tx.call(self, &mut tx_ope, ctx))
            .catch_unwind()
            .await;

        let err: Error;
        match maybe_panic_in_tx {
            Ok(result) => match result {
                Ok(success_value) => {
                    if tx_ope.operations.len() > MAX_BATCH_WRTIE_SIZE {
                        return Err(anyhow!(
                            "max batch write in transaction size = {} but passed {}",
                            MAX_BATCH_WRTIE_SIZE,
                            tx_ope.operations.len()
                        ));
                    }

                    self.commit(tx_ope.operations, Some(tx_ope.transaction))
                        .await?;
                    return Ok(success_value);
                }
                Err(e) => err = e,
            },
            Err(e) => err = anyhow!("panic occured in tx. rollback : {:?}", e),
        }

        // TODO(tacogips) need backoff?
        self.rollback(tx_ope.transaction).await?;
        Err(err)
    }

    pub async fn begin_transaction(&mut self) -> Result<Vec<u8>> {
        self.firestore_client
            .begin_transaction(request::new_begin_transaction_request(
                self.project_id.clone(),
                None,
            ))
            .await
            .map(|resp| resp.into_inner().transaction)
            .map_err(|e| Error::from(GrpcErrorStatus::from(e)))
    }

    pub async fn commit(
        &mut self,
        operations: Vec<request::DocumentWriteOperation>,
        transaction: Option<Vec<u8>>,
    ) -> Result<Vec<WriteResult>> {
        self.firestore_client
            .commit(request::new_commit_request(
                self.project_id.clone(),
                operations,
                transaction,
            ))
            .await
            .map(|resp| resp.into_inner().write_results)
            .map_err(|e| Error::from(GrpcErrorStatus::from(e)))
    }

    pub async fn rollback(&mut self, transaction: Vec<u8>) -> Result<()> {
        self.firestore_client
            .rollback(request::new_rollback_request(
                self.project_id.clone(),
                transaction,
            ))
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| Error::from(GrpcErrorStatus::from(e)))
    }

    pub async fn search_prefix_like<F>(
        &mut self,
        parent_path: Option<String>,
        collection: String,
        field: &str,
        prefix: &str,
        contain_exact_match: bool,
        transaction: Option<Vec<u8>>,
        mut with_each_doc: F,
    ) -> Result<i64>
    where
        F: FnMut(Document) -> Result<()>,
    {
        let query = QueryBuilder::collection(collection, false)
            .filter_bin(field, ">=", prefix.clone())
            .build();

        let mut result_num = 0;
        let mut result_stream = self
            .firestore_client
            .run_query(request::new_query_request(
                self.project_id.clone(),
                parent_path.unwrap_or("".to_owned()),
                query,
                transaction,
            ))
            .await?
            .into_inner();

        while let Some(each_response) = result_stream.message().await? {
            match each_response.document {
                Some(doc) => {
                    // check prefix
                    match doc.fields.get(field) {
                        None => break,
                        Some(field_value) => match FValue::from(field_value.clone()).as_string() {
                            None => break,
                            Some(str_value) => {
                                if !str_value.starts_with(prefix) {
                                    break;
                                }
                                if !contain_exact_match && str_value == prefix {
                                    continue;
                                }
                            }
                        },
                    }

                    result_num += 1;
                    with_each_doc(doc)?;
                }
                None => continue, //TODO(need to be interept?)
            }
        }
        Ok(result_num)
    }

    pub async fn run_query<F>(
        &mut self,
        parent_path: Option<String>,
        query: StructuredQuery,
        transaction: Option<Vec<u8>>,
        mut with_each_doc: F,
    ) -> Result<i64>
    where
        F: FnMut(Document) -> Result<()>,
    {
        let mut result_num = 0;
        let mut result_stream = self
            .firestore_client
            .run_query(request::new_query_request(
                self.project_id.clone(),
                parent_path.unwrap_or("".to_owned()),
                query,
                transaction,
            ))
            .await?
            .into_inner();

        while let Some(each_response) = result_stream.message().await? {
            match each_response.document {
                Some(doc) => {
                    result_num += 1;
                    with_each_doc(doc)?
                }
                None => continue, //TODO(need to be interept?)
            }
        }
        Ok(result_num)
    }

    pub async fn partition_query_all(
        &mut self,
        document_path: String,
        query: StructuredQuery,
        max_partition_count: i64,
        chunk_size: i32,
    ) -> Result<Vec<Cursor>> {
        let next_token = "".to_owned();
        fetch_through_all_tokens!(Cursor, {
            self.partition_query_chunk(
                document_path.clone(),
                query.clone(),
                max_partition_count,
                chunk_size,
                next_token.clone(),
            )
            .await?
        });
    }

    pub async fn partition_query_chunk(
        &mut self,
        document_path: String,
        query: StructuredQuery,
        max_partition_count: i64,
        chunk_size: i32,
        token: String,
    ) -> Result<(Vec<Cursor>, String)> {
        return self
            .firestore_client
            .partition_query(request::new_partition_query_request(
                self.project_id.clone(),
                document_path,
                query,
                max_partition_count,
                chunk_size,
                token,
            ))
            .await
            .map(|resp| {
                let result = resp.into_inner();
                (result.partitions, result.next_page_token)
            })
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    pub async fn update_document<D>(
        &mut self,
        document_path: String,
        document: D,
        update_field_mask: Option<Vec<String>>,
        response_field_mask: Option<Vec<String>>,
    ) -> Result<Document>
    where
        D: Into<HashMap<String, Value>>,
    {
        return self
            .firestore_client
            .update_document(request::new_update_document_request(
                self.project_id.clone(),
                document_path,
                document.into(),
                update_field_mask,
                response_field_mask,
            ))
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    pub async fn delete_document(&mut self, document_path: String) -> Result<()> {
        return self
            .firestore_client
            .delete_document(request::new_delete_document_request(
                self.project_id.clone(),
                document_path,
            ))
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    pub async fn create_document<D>(
        &mut self,
        parent_path: Option<String>,
        collection_id: String,
        document_id: String,
        document: D,
    ) -> Result<Document>
    where
        D: Into<HashMap<String, Value>>,
    {
        return self
            .firestore_client
            .create_document(request::new_create_document_request(
                self.project_id.clone(),
                parent_path.unwrap_or("".to_owned()),
                collection_id,
                document_id,
                document.into(),
                None,
            ))
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    //TODO(tacogips)
    pub async fn stream_write<F>(
        &mut self,
        _operations: impl Stream<Item = Vec<request::DocumentWriteOperation>> + Unpin,
        _with_each_response: F,
        _stream_id: Option<String>,
        _stream_token: Option<Vec<u8>>,
    ) -> Result<usize>
    where
        F: FnMut(Vec<WriteResult>) -> Result<()>,
    {
        unimplemented!(
            "could not write without error. The Firestore stream write API might be broken? "
        )
        //if stream_id.is_none() {
        //    let mut reqs = Vec::new();
        //    reqs.push(request::new_start_stream_write_request(
        //        self.project_id.clone(),
        //        stream_id.clone(),
        //    ));

        //    let mut begin_stream_reasult = self
        //        .firestore_client
        //        .write(stream::iter(reqs))
        //        .await?
        //        .into_inner();
        //    while let Some(resp) = begin_stream_reasult.message().await? {
        //        stream_id = Some(resp.stream_id);
        //        stream_token = Some(resp.stream_token);
        //    }
        //}

        //let mut result_num: usize = 0;

        //while let Some(each_opes) = operations.next().await {
        //    let mut reqs = Vec::new();
        //    let req = request::new_stream_write_request(
        //        self.project_id.clone(),
        //        each_opes,
        //        stream_id.clone().unwrap(),
        //        stream_token.clone().unwrap(),
        //    );
        //    reqs.push(req);

        //    let mut stream_result = self
        //        .firestore_client
        //        .write(stream::iter(reqs))
        //        .await?
        //        .into_inner();

        //    while let Some(each_response) = stream_result.message().await? {
        //        stream_id = Some(each_response.stream_id);
        //        stream_token = Some(each_response.stream_token);

        //        result_num += each_response.write_results.len();
        //        with_each_response(each_response.write_results)?;
        //    }
        //}

        //let mut reqs = Vec::new();
        //reqs.push(request::new_finish_stream_write_request(
        //    self.project_id.clone(),
        //    stream_token.unwrap(),
        //));

        //let mut finish_stream_reasult = self
        //    .firestore_client
        //    .write(stream::iter(reqs))
        //    .await?
        //    .into_inner();
        //if let Ok(Some(resp)) = finish_stream_reasult.message().await {
        //    Ok(result_num)
        //} else {
        //    Err(anyhow!("failed to finish stream"))
        //}
    }

    pub async fn large_batch_write(
        &mut self,
        operations: Vec<request::DocumentWriteOperation>,
    ) -> Result<Vec<WriteResult>> {
        let mut result = Vec::new();
        for chunk in operations.chunks(MAX_BATCH_WRTIE_SIZE).into_iter() {
            let mut each_result = self.batch_write(chunk.to_vec()).await?;
            result.append(&mut each_result)
        }
        Ok(result)
    }

    pub async fn batch_write(
        &mut self,
        operations: Vec<request::DocumentWriteOperation>,
    ) -> Result<Vec<WriteResult>> {
        if operations.len() > MAX_BATCH_WRTIE_SIZE {
            return Err(anyhow!(
                "max batch write size = {} but passed {}",
                MAX_BATCH_WRTIE_SIZE,
                operations.len()
            ));
        }

        return self
            .firestore_client
            .batch_write(request::new_batch_write_request(
                self.project_id.clone(),
                operations,
            ))
            .await
            .map(|resp| resp.into_inner().write_results)
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    pub async fn batch_get_documents<F>(
        &mut self,
        document_paths: Vec<String>,
        field_mask: Option<Vec<String>>,
        transaction: Option<Vec<u8>>,
        mut with_each_doc: F,
    ) -> Result<MissingDocPaths>
    where
        F: FnMut(Document) -> Result<()>,
    {
        let mut missing_doc_paths = Vec::<String>::new();
        for each_document_paths in document_paths
            .chunks(MAX_BATCH_GET_DOC_NUM)
            .into_iter()
            .map(|doc_ids| doc_ids.to_vec())
        {
            let mut result_stream = self
                .firestore_client
                .batch_get_documents(request::new_batch_get_documents_request(
                    self.project_id.clone(),
                    each_document_paths,
                    field_mask.clone(),
                    transaction.clone(),
                ))
                .await?
                .into_inner();

            while let Some(each_response) = result_stream.message().await? {
                match each_response.result {
                    Some(doc_result) => match doc_result {
                        DocResult::Found(doc) => with_each_doc(doc)?,
                        DocResult::Missing(doc_id) => {
                            missing_doc_paths.push(doc_id);
                            continue;
                        }
                    },

                    None => {
                        log::warn!("batch get document return none result");
                        break;
                    }
                }
            }
        }

        if missing_doc_paths.is_empty() {
            Ok([].to_vec())
        } else {
            Ok(missing_doc_paths)
        }
    }

    pub async fn get_document(
        &mut self,
        document_path: String,
        field_mask: Option<Vec<String>>,
        transaction: Option<Vec<u8>>,
    ) -> Result<Option<Document>> {
        match self
            .firestore_client
            .get_document(request::new_get_document_request(
                self.project_id.clone(),
                document_path,
                field_mask,
                transaction,
            ))
            .await
            .map(|resp| resp.into_inner())
        {
            Ok(found) => Ok(Some(found)),
            Err(status) => {
                if status.code() == Code::NotFound {
                    Ok(None)
                } else {
                    Err(GrpcErrorStatus::from(status).into())
                }
            }
        }
    }

    pub async fn list_documents_all(
        &mut self,
        parent_path: Option<String>,
        collection_id: String,
        order_by: Option<String>,
        chunk_size: Option<i32>,
        field_mask: Option<Vec<String>>,
        transaction: Option<Vec<u8>>,
    ) -> Result<Vec<Document>> {
        let next_token = "".to_owned();
        fetch_through_all_tokens!(Document, {
            self.list_documents_chunk(
                parent_path.clone(),
                collection_id.clone(),
                order_by.clone(),
                chunk_size.clone(),
                field_mask.clone(),
                transaction.clone(),
                next_token.clone(),
            )
            .await?
        });
    }

    pub async fn list_documents_chunk(
        &mut self,
        parent_path: Option<String>,
        collection_id: String,
        order_by: Option<String>,
        chunk_size: Option<i32>,
        field_mask: Option<Vec<String>>,
        transaction: Option<Vec<u8>>,
        page_token: String,
    ) -> Result<(Vec<Document>, String)> {
        return self
            .firestore_client
            .list_documents(request::new_list_document_request(
                self.project_id.clone(),
                parent_path.unwrap_or("".to_owned()),
                collection_id,
                page_token,
                order_by,
                chunk_size,
                field_mask,
                transaction,
            ))
            .await
            .map(|resp| {
                let resp = resp.into_inner();
                (resp.documents, resp.next_page_token)
            })
            .map_err(|e| GrpcErrorStatus::from(e).into());
    }

    pub async fn list_collection_ids_all<F>(
        &mut self,
        project_id: String,
        document_path: String,
        chunk_size: Option<i32>,
        filter_fn: F,
    ) -> Result<Vec<String>>
    where
        F: for<'a> FnMut(&'a String) -> bool + Copy,
    {
        let next_token = "".to_owned();
        fetch_through_all_tokens!(String, {
            self.list_collection_ids_chunks(
                project_id.clone(),
                document_path.clone(),
                chunk_size,
                filter_fn,
                next_token.clone(),
            )
            .await?
        });
    }

    pub async fn list_collection_ids_chunks<F>(
        &mut self,
        project_id: String,
        document_path: String,
        chunk_size: Option<i32>,
        filter_fn: F,
        token: String,
    ) -> Result<(Vec<String>, String)>
    where
        F: for<'a> FnMut(&'a String) -> bool + Copy,
    {
        let req = request::new_collection_ids_request(project_id, document_path, chunk_size, token);

        let response = self.firestore_client.list_collection_ids(req).await?;
        let response = response.into_inner();
        let next_token = response.next_page_token;
        let items = response
            .collection_ids
            .into_iter()
            .filter(filter_fn)
            .collect();

        Ok((items, next_token))
    }
}

/// clone firestore client so send multi request by one client
///https://github.com/hyperium/tonic/issues/240
impl Clone for FirestoreClient {
    fn clone(&self) -> Self {
        //TODO (tacogips)
        // check that the atomic shared_token in interceptor(actualy closure) in firestore_client
        // also will be cloned equally as Arc::clone()
        Self {
            project_id: self.project_id.clone(),
            firestore_client: self.firestore_client.clone(),
            token_manager: Arc::clone(&self.token_manager),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{request, FirestoreClient, TransactionOperation};

    use std::path::Path;

    use anyhow::{anyhow, Result};
    use std::env;
    use uuid::Uuid;

    const TEST_COLLECTION_ID: &str = "test_coll";

    use super::super::query::QueryBuilder;

    use crate::firestore::{
        value::{array_value_from_vec, doc_path, map_value_from_vec, FFields, FValue},
        FDocument,
    };

    fn test_service_account_path() -> String {
        env::var("TEST_SERVICE_ACCOUT").unwrap()
    }

    fn test_project_id() -> String {
        env::var("TEST_PROJECT_ID").unwrap()
    }

    #[tokio::test]
    async fn collection_ids() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();
        let coll_ids = cli
            .list_collection_ids_all(
                test_project_id().to_owned(),
                "".into(),
                None,
                super::id_filter(),
            )
            .await;

        assert!(coll_ids.is_ok());
    }

    #[tokio::test]
    async fn list_documents() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        let collection_id = TEST_COLLECTION_ID.to_owned();

        let doc_id_1 = format!("list_docuemnt_{}", Uuid::new_v4().to_urn());
        let doc_id_2 = format!("list_docuemnt_{}", Uuid::new_v4().to_urn());
        let doc_id_3 = format!("list_docuemnt_{}", Uuid::new_v4().to_urn());
        let create_ope_1 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_1.clone(),
                fields,
            )
        };
        let create_ope_2 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_2.clone(),
                fields,
            )
        };

        let create_ope_3 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), Option::<i64>::None);

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_3.clone(),
                fields,
            )
        };

        {
            cli.batch_write(vec![create_ope_1, create_ope_2, create_ope_3])
                .await
                .unwrap();
        }
        let result = cli
            .list_documents_all(None, collection_id.clone(), None, None, None, None)
            .await
            .unwrap();

        assert_ne!(0, result.len());

        {
            //delete
            let result = cli
                .delete_document(doc_path(None, collection_id.clone(), doc_id_1.clone()))
                .await
                .is_ok();
            assert!(result);

            let result = cli
                .delete_document(doc_path(None, collection_id.clone(), doc_id_2.clone()))
                .await
                .is_ok();
            assert!(result);

            let result = cli
                .delete_document(doc_path(None, collection_id.clone(), doc_id_3.clone()))
                .await
                .is_ok();
            assert!(result);
        }
    }

    #[tokio::test]
    async fn crud_object() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        let collection_id = TEST_COLLECTION_ID.to_owned();
        let doc_id = format!("doc_{}", Uuid::new_v4().to_urn());

        let mut fields = FFields::empty();
        fields.add("ssss".to_owned(), "asdf".to_owned());
        fields.add(
            "aaa".to_owned(),
            array_value_from_vec(vec![FValue::from("ddd".to_owned()), FValue::from(333.3f64)]),
        );
        fields.add(
            "bbb".to_owned(),
            map_value_from_vec(vec![
                ("k".to_owned(), FValue::from("bbb".to_owned())),
                ("sa".to_owned(), FValue::from(333i64)),
                ("d".to_owned(), FValue::from(333.3f64)),
            ]),
        );

        {
            // create
            let doc = cli
                .create_document(None, TEST_COLLECTION_ID.to_owned(), doc_id.clone(), fields)
                .await
                .unwrap();

            let fields = FFields::from_grpc_doc(doc);
            let ssss = fields.get("ssss");
            let ssss: &FValue = ssss.unwrap();
            let s_value: Option<&String> = ssss.as_string();

            assert_eq!("asdf".to_owned(), *s_value.unwrap());
        }

        {
            //get
            let doc = cli
                .get_document(
                    doc_path(None, collection_id.clone(), doc_id.clone()),
                    None,
                    None,
                )
                .await
                .unwrap();
            let ffields = FFields::from_grpc_doc(doc.unwrap());
            let actual = ffields.get("ssss").unwrap().as_string().unwrap();
            assert_eq!("asdf".to_owned(), *actual);
        }

        {
            //update
            //
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), FValue::NullValue);
            let doc = cli
                .update_document(
                    doc_path(None, collection_id.clone(), doc_id.clone()),
                    fields,
                    None,
                    None,
                )
                .await
                .unwrap();
            let ffields = FFields::from_grpc_doc(doc);
            assert!(ffields.get("ssss").is_none());
        }

        {
            //get
            let doc = cli
                .get_document(
                    doc_path(None, collection_id.clone(), doc_id.clone()),
                    None,
                    None,
                )
                .await
                .unwrap();
            let ffields = FFields::from_grpc_doc(doc.unwrap());
            assert!(ffields.get("ssss").is_none());
            assert_eq!(*ffields.get("bbb").unwrap(), FValue::NullValue);
        }

        {
            //delete
            let result = cli
                .delete_document(doc_path(None, collection_id.clone(), doc_id.clone()))
                .await
                .is_ok();
            assert!(result);
        }

        {
            //get (not found)
            let doc = cli
                .get_document(
                    doc_path(None, collection_id.clone(), doc_id.clone()),
                    None,
                    None,
                )
                .await
                .unwrap();
            assert!(doc.is_none())
        }
    }

    #[tokio::test]
    async fn batch_update_object() {
        use crate::firestore::value::{
            array_value_from_vec, doc_path, map_value_from_vec, FDocument, FDocumentPath, FFields,
            FValue,
        };

        let cred_path = test_service_account_path();

        let cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();
        let mut cli = cli.clone();

        let collection_id = TEST_COLLECTION_ID.to_owned();
        let doc_id = format!("doc_{}", Uuid::new_v4().to_urn());
        let doc_id_2 = format!("doc_{}", Uuid::new_v4().to_urn());

        let mut fields = FFields::empty();
        fields.add("ssss".to_owned(), "asdf".to_owned());
        fields.add(
            "aaa".to_owned(),
            array_value_from_vec(vec![FValue::from("ddd".to_owned()), FValue::from(333.3f64)]),
        );
        fields.add(
            "bbb".to_owned(),
            map_value_from_vec(vec![
                ("k".to_owned(), FValue::from("bbb".to_owned())),
                ("sa".to_owned(), FValue::from(333i64)),
                ("d".to_owned(), FValue::from(333.3f64)),
            ]),
        );

        // create
        {
            let result = cli
                .create_document(None, TEST_COLLECTION_ID.to_owned(), doc_id.clone(), fields)
                .await;

            assert!(result.is_ok());
        }

        {
            // update and create
            let mut update_fields = FFields::empty();
            update_fields.add("ssss".to_owned(), "bbbb".to_owned());
            let update_ope = request::DocumentWriteOperation::new_upsert(
                doc_path(None, collection_id.clone(), doc_id.clone()),
                update_fields,
            );

            let mut create_fields = FFields::empty();
            create_fields.add("aaaa".to_owned(), 1111);

            let create_ope = request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_2.clone(),
                create_fields,
            );

            let write_results = cli.batch_write(vec![create_ope, update_ope]).await.unwrap();
            let write_results = FValue::from_write_results(write_results);

            // operations without transforming returns result with 0
            assert_eq!(0, write_results[0].len());
        }

        {
            let doc_path_1 = doc_path(None, collection_id.clone(), doc_id.clone());
            let doc_path_2 = "/not_exists_coll/some".to_owned();
            let doc_path_3 = doc_path(None, collection_id.clone(), doc_id_2.clone());
            let mut result: Vec<FDocument> = Vec::new();

            let missing_doc_paths = cli
                .batch_get_documents(vec![doc_path_1, doc_path_2, doc_path_3], None, None, |e| {
                    result.push(FDocument::from_document(e).unwrap());
                    Ok(())
                })
                .await
                .unwrap();

            assert_eq!(2, result.len());

            let missing_doc_paths = missing_doc_paths;
            assert_eq!(1, missing_doc_paths.len());

            let actual_missing_path = FDocumentPath::parse(&missing_doc_paths[0])
                .unwrap()
                .into_string();
            assert_eq!("/not_exists_coll/some", actual_missing_path,);
        }

        {
            //batch delete
            let doc_path_1 = doc_path(None, collection_id.clone(), doc_id.clone());
            let doc_path_2 = doc_path(None, collection_id.clone(), doc_id_2.clone());
            let delete_ope_1 = request::DocumentWriteOperation::new_delete(doc_path_1);
            let delete_ope_2 = request::DocumentWriteOperation::new_delete(doc_path_2);

            let write_results = cli
                .batch_write(vec![delete_ope_1, delete_ope_2])
                .await
                .unwrap();
            let write_results = FValue::from_write_results(write_results);
            // operations without transforming returns result with 0
            assert_eq!(0, write_results[0].len());
        }

        {
            let doc_path_1 = doc_path(None, collection_id.clone(), doc_id.clone());
            let doc_path_2 = doc_path(None, collection_id.clone(), doc_id_2.clone());

            let missing_doc_paths = cli
                .batch_get_documents(
                    vec![doc_path_1.clone(), doc_path_2.clone()],
                    None,
                    None,
                    |_| {
                        assert!(false, "must not called");
                        Ok(())
                    },
                )
                .await
                .unwrap();

            assert_eq!(2, missing_doc_paths.len());
        }
    }

    #[tokio::test]
    async fn write_in_transaction() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        {
            // create and delete in transaction
            let doc_id = format!("doc_{}", Uuid::new_v4().to_urn());

            struct DocID {
                doc_id: String,
            }

            let ctx = DocID { doc_id };

            async fn trans_ope(
                cli_in_tx: &mut FirestoreClient,
                tx: &mut TransactionOperation,
                ctx: DocID,
            ) -> Result<i32> {
                let collection_id = TEST_COLLECTION_ID.to_owned();
                let mut fields = FFields::empty();
                fields.add("ssss".to_owned(), "asdf".to_owned());
                cli_in_tx
                    .create_document(None, collection_id.clone(), ctx.doc_id.clone(), fields)
                    .await;

                tx.add_operation(request::DocumentWriteOperation::new_delete(doc_path(
                    None,
                    collection_id.clone(),
                    ctx.doc_id.clone(),
                )));

                Ok(100i32)
            }

            let result = cli.in_transaction(ctx, trans_ope).await;
            assert_eq!(100i32, result.unwrap());
        }
    }

    #[tokio::test]
    async fn error_in_transaction() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        {
            // create and delete in transaction
            let doc_id = format!("doc_not_created_{}", Uuid::new_v4().to_urn());

            struct DocID {
                doc_id: String,
            }

            let ctx = DocID { doc_id };

            async fn trans_ope(
                _: &mut FirestoreClient,
                tx: &mut TransactionOperation,
                ctx: DocID,
            ) -> Result<i32> {
                let collection_id = TEST_COLLECTION_ID.to_owned();
                let mut fields = FFields::empty();
                fields.add("bbb".to_owned(), "ssss".to_owned());
                tx.add_operation(request::DocumentWriteOperation::new_create(
                    None,
                    collection_id.clone(),
                    ctx.doc_id.clone(),
                    fields,
                ));

                Err(anyhow!("something went wrong"))
            }

            let result = cli.in_transaction(ctx, trans_ope).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn panic_in_transaction() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        {
            // create and delete in transaction
            let doc_id = format!("doc_not_created_{}", Uuid::new_v4().to_urn());

            struct DocID {
                doc_id: String,
            }

            let ctx = DocID { doc_id };

            async fn trans_ope(
                _: &mut FirestoreClient,
                tx: &mut TransactionOperation,
                ctx: DocID,
            ) -> Result<i32> {
                let collection_id = TEST_COLLECTION_ID.to_owned();
                let mut fields = FFields::empty();
                fields.add("bbb".to_owned(), "ssss".to_owned());
                tx.add_operation(request::DocumentWriteOperation::new_create(
                    None,
                    collection_id.clone(),
                    ctx.doc_id.clone(),
                    fields,
                ));
                panic!("something went south");

                Ok(111i32)
            }

            let result = cli.in_transaction(ctx, trans_ope).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn query_test() {
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        let collection_id = TEST_COLLECTION_ID.to_owned();

        let map_value =
            map_value_from_vec(vec![("this", 123f64), ("is", 999f64), ("map", 7777f64)]);

        let doc_id_1 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let doc_id_2 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let doc_id_3 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let create_ope_1 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());
            fields.add("cccc".to_owned(), vec!["hello", "what's", "up"]);
            fields.add("dddd".to_owned(), map_value);

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_1.clone(),
                fields,
            )
        };
        let create_ope_2 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());
            fields.add("cccc".to_owned(), vec!["oh", "my"]);
            fields.add("aaa".to_owned(), 123f64.to_owned());

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_2.clone(),
                fields,
            )
        };

        let create_ope_3 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), Option::<i64>::None);

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_3.clone(),
                fields,
            )
        };

        {
            cli.batch_write(vec![create_ope_1, create_ope_2, create_ope_3])
                .await
                .unwrap();
        }

        {
            let q = QueryBuilder::collection(collection_id.clone(), false)
                .filter_bin("bbb", "==", "ssss".to_owned())
                .filter_bin("cccc", "array-contains", "hello".to_owned())
                .build();
            let result = cli
                .run_query(None, q, None, |doc| {
                    let doc = FDocument::from(doc);
                    assert_eq!(doc_id_1.clone(), doc.doc_path.document_id);
                    Ok(())
                })
                .await;
            assert_eq!(1, result.unwrap())
        }

        {
            {
                //batch delete
                let doc_path_1 = doc_path(None, collection_id.clone(), doc_id_1.clone());
                let doc_path_2 = doc_path(None, collection_id.clone(), doc_id_2.clone());
                let doc_path_3 = doc_path(None, collection_id.clone(), doc_id_3.clone());
                let delete_ope_1 = request::DocumentWriteOperation::new_delete(doc_path_1);
                let delete_ope_2 = request::DocumentWriteOperation::new_delete(doc_path_2);
                let delete_ope_3 = request::DocumentWriteOperation::new_delete(doc_path_3);

                let write_results = cli
                    .batch_write(vec![delete_ope_1, delete_ope_2, delete_ope_3])
                    .await
                    .unwrap();
                let write_results = FValue::from_write_results(write_results);
                // operations without transforming returns result with 0
                assert_eq!(0, write_results[0].len());
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn query_stream() {
        use futures_util::stream;
        let cred_path = test_service_account_path();

        let mut cli = super::FirestoreClient::with_service_account_file(
            test_project_id().to_owned(),
            Path::new(&cred_path).to_path_buf(),
        )
        .await
        .unwrap();

        let collection_id = TEST_COLLECTION_ID.to_owned();

        let map_value =
            map_value_from_vec(vec![("this", 123f64), ("is", 999f64), ("map", 7777f64)]);

        let doc_id_1 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let doc_id_2 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let doc_id_3 = format!("doc_not_created_{}", Uuid::new_v4().to_urn());
        let create_ope_1 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());
            fields.add("cccc".to_owned(), vec!["hello", "what's", "up"]);
            fields.add("dddd".to_owned(), map_value);

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_1.clone(),
                fields,
            )
        };
        let create_ope_2 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), "ssss".to_owned());
            fields.add("cccc".to_owned(), vec!["oh", "my"]);
            fields.add("aaa".to_owned(), 123f64.to_owned());

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_2.clone(),
                fields,
            )
        };

        let create_ope_3 = {
            let mut fields = FFields::empty();
            fields.add("bbb".to_owned(), Option::<i64>::None);

            request::DocumentWriteOperation::new_create(
                None,
                collection_id.clone(),
                doc_id_3.clone(),
                fields,
            )
        };

        {
            let response = cli
                .stream_write(
                    stream::iter(vec![vec![create_ope_1, create_ope_2, create_ope_3]]),
                    |each| Ok(()),
                    None,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(3, response);
        }
    }
}
