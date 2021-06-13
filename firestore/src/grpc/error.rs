use google_cloud_grpc_proto::tonic::Status;
use std::convert::From;
use std::fmt;

use std::error::Error;

#[derive(Debug)]
pub struct GrpcErrorStatus(Status);

impl From<Status> for GrpcErrorStatus {
    fn from(status: Status) -> Self {
        GrpcErrorStatus(status)
    }
}

impl fmt::Display for GrpcErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl Error for GrpcErrorStatus {}
