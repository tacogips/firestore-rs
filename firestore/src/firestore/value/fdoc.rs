use super::grpc_values::Document;
use super::{fvalue::FValue, FFields};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    //TODO(tacogips) needs more strict matching accoding to https://firebase.google.com/docs/firestore/quotas
    static ref DOCUMENT_ID_REGEX: Regex =
        Regex::new(r".*?/.*?/documents(.*)/([^/].*?)/([^/].*?)$").unwrap();
}

fn parse_document_path(path: &str) -> Result<(Option<String>, String, String)> {
    DOCUMENT_ID_REGEX
        .captures(path)
        .map_or(Err(anyhow!("invalid doc path {}", path)), |captured| {
            let parent_path = captured
                .get(1)
                .map(|m| m.as_str())
                .ok_or_else(|| anyhow!("invalid doc path {}", path))?;

            let parent_path = if parent_path.is_empty() {
                None
            } else {
                Some(parent_path.to_owned())
            };

            let collection_id = captured
                .get(2)
                .map(|m| m.as_str().to_owned())
                .ok_or_else(|| anyhow!("invalid doc path {}", path))?;

            let doc_id = captured
                .get(3)
                .map(|m| m.as_str().to_owned())
                .ok_or_else(|| anyhow!("invalid doc path {}", path))?;
            Ok((parent_path, collection_id, doc_id))
        })
}

#[derive(Debug, PartialEq)]
pub struct FDocumentPath {
    pub parent_path: Option<String>,
    pub collection_id: String,
    pub document_id: String,
}

impl FDocumentPath {
    pub fn new(parent_path: Option<String>, collection_id: String, document_id: String) -> Self {
        Self {
            parent_path,
            collection_id,
            document_id,
        }
    }

    pub fn into_string(self) -> String {
        format!(
            "{}/{}/{}",
            self.parent_path.unwrap_or("".to_owned()),
            self.collection_id,
            self.document_id
        )
    }

    pub fn parse(path: &str) -> Result<Self> {
        let (parent_path, collection_id, document_id) = parse_document_path(path)?;

        Ok(FDocumentPath {
            parent_path,
            collection_id,
            document_id,
        })
    }
}

pub fn doc_path(parent: Option<String>, collection_id: String, doc_id: String) -> String {
    format!(
        "{}/{}/{}",
        parent.unwrap_or("".to_owned()),
        collection_id,
        doc_id
    )
}

#[derive(Debug)]
pub struct FDocument {
    pub doc_path: FDocumentPath,
    pub fields: FFields,
}

impl FDocument {
    pub fn from_document(document: Document) -> Result<FDocument> {
        let doc_path = FDocumentPath::parse(document.name.as_str())?;
        let fields = FFields::from_grpc_doc(document);

        Ok(FDocument { doc_path, fields })
    }

    pub fn to_path_and_fvalue(self) -> (FDocumentPath, FValue) {
        let fvalue: FValue = self.fields.into();
        (self.doc_path, fvalue)
    }
}

impl From<Document> for FDocument {
    fn from(document: Document) -> FDocument {
        let doc_path = FDocumentPath::parse(document.name.as_str()).unwrap();
        let fields = FFields::from_grpc_doc(document);

        FDocument { doc_path, fields }
    }
}

impl Into<FValue> for FDocument {
    fn into(self) -> FValue {
        FValue::Map(self.fields.into())
    }
}

#[cfg(test)]
mod test {
    use super::parse_document_path;
    #[test]
    fn parse_doc_path_test() {
        {
            let (parent, col_id, doc_id) =
                parse_document_path("projects/aaa/databases/(default)/documents/coll_1/doc_1")
                    .unwrap();
            assert_eq!(None, parent);
            assert_eq!("coll_1", col_id);
            assert_eq!("doc_1", doc_id);
        }

        {
            let (parent, col_id, doc_id) = parse_document_path(
                "projects/aaa/databases/(default)/documents/coll_1/doc_1/coll_2/doc_2",
            )
            .unwrap();
            assert_eq!(Some("/coll_1/doc_1".to_owned()), parent);
            assert_eq!("coll_2", col_id);
            assert_eq!("doc_2", doc_id);
        }

        {
            let (parent, col_id, doc_id) =
                parse_document_path("projects/aaa/databases/(default)/documents/documents/doc_1")
                    .unwrap();
            assert_eq!(None, parent);
            assert_eq!("documents", col_id);
            assert_eq!("doc_1", doc_id);
        }

        {
            let result = parse_document_path("projects/aaa/databases/(default)/documents/coll_1");
            assert!(result.is_err())
        }

        {
            let result = parse_document_path("projects/aaa/databases/(default)/documents/coll_1/");
            assert!(result.is_err())
        }

        {
            let result = parse_document_path("projects/aaa/databases/(default)/documents///");
            assert!(result.is_err())
        }
    }
}
