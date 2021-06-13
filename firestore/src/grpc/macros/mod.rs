//TODO(tacogips) this macro won't work ??
//#[macro_export]
/////
///// fetch_through_all_tokens!(ResponseElemType, chunk_fetching_body)
///// chunk_fetching_body  expected to retrun (chunk,Vec<ResponseElemType>,next_token:String)
///// example.
/////  ```rust
/////     use google_cloud_grpc::fetch_through_all_tokens;
/////     use anyhow::Result;
/////
/////     async fn fetch_chunk(
/////                 project_id:String,
/////                 token:String)->Result<(Vec<String>, String)>{
/////         Ok((vec!["doc1".to_owned(),"doc2".to_owned()],
/////             "the_next_token_123".to_owned()))
/////     }
/////
/////     async fn list_collection_ids_all<F>(
/////         project_id: String,
/////     ) -> Result<Vec<String>> {
/////         let next_token = "".to_owned();
/////         fetch_through_all_tokens!(String, {
/////             fetch_chunk(
/////                 project_id.clone(),
/////                 next_token.clone(),
/////             )
/////             .await?
/////         });
/////     }
///// ```
//macro_rules! fetch_through_all_tokens {
//    ($resp_type:ty, $body:expr) => {
//        let ref mut next_token = "".to_owned();
//        let mut result = Vec::<$resp_type>::new();
//        loop {
//            let response = $body;
//
//            let mut items = response.0;
//            *next_token = response.1;
//
//            result.append(&mut items);
//
//            if next_token.is_empty() {
//                break;
//            }
//        }
//        return Ok(result)
//    };
//}
