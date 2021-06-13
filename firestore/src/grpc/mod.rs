use anyhow::Result;
use google_cloud_grpc_proto::tonic::transport::{Channel, ClientTlsConfig};

pub(crate) mod auth;
pub(crate) mod connection_point;
use connection_point::GrpcConnectionPoint;

pub struct GrpcChannel {
    pub opened_channel: Option<Channel>,
}
impl GrpcChannel {
    pub async fn new_connected_channnel(
        connection_point: &GrpcConnectionPoint,
    ) -> Result<GrpcChannel> {
        let opened_channel = Self::connect(connection_point).await?;
        Ok(GrpcChannel {
            opened_channel: Some(opened_channel),
        })
    }

    async fn connect(connection_point: &GrpcConnectionPoint) -> Result<Channel> {
        let GrpcConnectionPoint(endpoint, domain) = *connection_point;
        let tls_config = ClientTlsConfig::new().domain_name(domain);
        let endpoint = Channel::from_static(endpoint).tls_config(tls_config)?;
        let channel = endpoint.connect().await?;
        Ok(channel)
    }
}
pub(crate) mod error;

pub(crate) mod macros;
