use anyhow::{anyhow, Result};
use arc_swap::ArcSwap;
use chrono::{offset::Utc, Duration};
use hyper;
use log;

//use async_std::sync::{Condvar, Mutex};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use yup_oauth2::{
    self as oauth,
    authenticator::{Authenticator, DefaultHyperClient, HyperClientBuilder},
    AccessToken,
};

use google_cloud_grpc_proto::tonic::{metadata::MetadataValue, Request, Status};

pub(crate) mod scopes;
use scopes::Scope;
use std::thread;

#[derive(Clone, Copy, Debug)]
pub struct TokenRefresh {
    do_auto_refresh: bool,
    refresh_in_minutes_to_expire: Duration,
    refresh_check_duration: Duration,
}

impl Default for TokenRefresh {
    fn default() -> Self {
        Self {
            do_auto_refresh: true,
            refresh_in_minutes_to_expire: Duration::minutes(5),
            refresh_check_duration: Duration::minutes(1),
        }
    }
}

#[allow(dead_code)]
pub struct TokenManager<HttpConnector> {
    authenticator: Arc<Authenticator<HttpConnector>>,
    scopes: Vec<Scope>,
    token_refresh: TokenRefresh,
    current_token: Arc<ArcSwap<AccessToken>>,
    finish_refreshing: Arc<(Mutex<bool>, Condvar)>,
    pub refresh_token_schedule_jh: std::thread::JoinHandle<()>,
    pub refresh_token_loop_jh: tokio::task::JoinHandle<()>,
    refresh_token_signal_sender: tokio::sync::mpsc::UnboundedSender<std::time::Instant>,
}

impl<HttpConnector> TokenManager<HttpConnector>
where
    HttpConnector: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    async fn start(
        authenticator: Authenticator<HttpConnector>,
        scopes: Vec<Scope>,
        token_refresh: TokenRefresh,
    ) -> Result<Self> {
        let access_token = authenticator.token(scopes.as_ref()).await?;
        let current_token = Arc::new(ArcSwap::from(Arc::new(access_token)));

        let finish_refreshing = Arc::new((Mutex::new(false), Condvar::new()));
        let authenticator = Arc::new(authenticator);

        let (refresh_token_signal_sender, refresh_token_schedule_jh, refresh_token_loop_jh) =
            Self::start_refreshing_token(
                Arc::clone(&authenticator),
                Arc::clone(&current_token),
                Arc::clone(&finish_refreshing),
                scopes.clone(),
                token_refresh.clone(),
            );

        let result = Self {
            authenticator,
            scopes,
            token_refresh,
            current_token,
            finish_refreshing,
            refresh_token_schedule_jh,
            refresh_token_loop_jh,
            refresh_token_signal_sender,
        };

        Ok(result)
    }

    pub fn start_refreshing_token(
        authenticator: Arc<Authenticator<HttpConnector>>,
        shared_token: Arc<ArcSwap<AccessToken>>,
        finish_refreshing: Arc<(Mutex<bool>, Condvar)>,
        scopes: Vec<Scope>,
        token_refresh: TokenRefresh,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<std::time::Instant>,
        std::thread::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<std::time::Instant>();
        // TODO (tacogips) https://docs.rs/tokio/0.2.25/tokio/task/fn.spawn_blocking.html
        // loop tokio::task::spawn_blocking a
        // current implementation based on comment on https://users.rust-lang.org/t/how-to-use-async-fn-in-thread-spawn/46413
        //
        //https://users.rust-lang.org/t/is-it-okay-to-use-infinite-loop-in-an-async-function/42385
        let schedule_tx = tx.clone();
        let shared_token_current = shared_token.clone();
        //TODO(tacogips ) could this variable couldn't be tokio::task::JoinHandle?
        let refresh_token_schedule_jh: std::thread::JoinHandle<()> = thread::spawn(move || {
            log::debug!("start gcp auth refresing ...");
            loop {
                let current_token = shared_token_current.load();
                let need_refresh = (**current_token)
                    .expiration_time()
                    .map(|expiration_time| {
                        expiration_time - token_refresh.refresh_in_minutes_to_expire <= Utc::now()
                    })
                    .unwrap_or(false);

                if need_refresh {
                    log::debug!("refreshing auth token of GCP");
                    schedule_tx.send(std::time::Instant::now()).unwrap()
                }

                log::debug!("fetch auth refreshing finish lock");
                let (finish_lock, cvar) = &*finish_refreshing;
                let mut finished = finish_lock.lock().unwrap();

                log::debug!("waiting auth refreshing");
                let waited = cvar
                    .wait_timeout(
                        finished,
                        token_refresh.refresh_check_duration.to_std().unwrap(),
                    )
                    .unwrap();
                finished = waited.0;

                log::debug!("check token manager finished? {}", *finished);
                if *finished {
                    log::info!("exit token refreshing loop");
                    break;
                }
            }
        });

        // TODO(tacogips) Is that OK that tokio::spawn contains loop in it.
        let refresh_token_loop_jh: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            while let Some(time) = rx.recv().await {
                log::info!("updating token at {:?}", time);
                //TODO(tacogips) need backoff
                let new_token = Self::get_new_token(&authenticator, &scopes).await;
                match new_token {
                    Ok(access_token) => shared_token.store(Arc::new(access_token)),
                    Err(e) => {
                        log::error!("failed to refresh token :{}", e);
                        thread::sleep(Duration::seconds(1).to_std().unwrap());
                        continue;
                    }
                }
            }

            log::info!("exit from refreshing token loop")
        });
        (tx, refresh_token_schedule_jh, refresh_token_loop_jh)
    }

    pub fn force_refresh_token(&self) -> Result<()> {
        self.refresh_token_signal_sender
            .send(std::time::Instant::now())?;
        Ok(())
    }

    pub async fn get_new_token(
        authenticator: &Authenticator<HttpConnector>,
        scopes: &[Scope],
    ) -> Result<AccessToken> {
        let new_token = authenticator.force_refreshed_token(scopes).await?;
        Ok(new_token)
    }

    pub fn shared_token(&self) -> Arc<ArcSwap<AccessToken>> {
        Arc::clone(&self.current_token)
    }

    pub fn refresh_token(&self) -> Arc<ArcSwap<AccessToken>> {
        Arc::clone(&self.current_token)
    }

    pub async fn stop_auth_refreshing(self) -> Result<()> {
        stop_auth_refreshing(self.finish_refreshing.clone());
        Ok(())
    }
}

pub fn stop_auth_refreshing(finish_refreshing: Arc<(Mutex<bool>, Condvar)>) {
    log::info!("dropping token manager");
    let (finish_lock, cvar) = &*finish_refreshing;
    let mut finish = finish_lock.lock().unwrap();
    *finish = true;
    cvar.notify_one()
}

impl<T> Drop for TokenManager<T> {
    fn drop(&mut self) {
        stop_auth_refreshing(self.finish_refreshing.clone())
    }
}

pub struct TokenManagerBuilder {
    scopes: Vec<Scope>,
    service_account_file_path: Option<PathBuf>,
    token_refresh: Option<TokenRefresh>,
}

impl TokenManagerBuilder {
    pub fn new(scopes: Vec<Scope>) -> Self {
        Self {
            scopes: scopes,
            service_account_file_path: None,
            token_refresh: None,
        }
    }
    pub fn service_account_file(self, path: PathBuf) -> Self {
        TokenManagerBuilder {
            service_account_file_path: Some(path),
            ..self
        }
    }

    pub async fn build(
        self,
    ) -> Result<TokenManager<<DefaultHyperClient as HyperClientBuilder>::Connector>> {
        let sa_path = self.service_account_file_path.clone();
        let sa_path = sa_path.ok_or_else(|| {
                anyhow!(
                "the service account is required to create authenticator.(in current impmlementation)"
            )
            })?;

        self.from_service_account_file(sa_path).await
    }

    async fn from_service_account_file(
        self,
        service_account_cred_file: PathBuf,
    ) -> Result<TokenManager<<DefaultHyperClient as HyperClientBuilder>::Connector>> {
        let sa_key = oauth::read_service_account_key(service_account_cred_file.clone())
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to read service account file at {}: {}",
                    service_account_cred_file.display(),
                    e.to_string()
                )
            })?;
        let auth = oauth::ServiceAccountAuthenticator::builder(sa_key)
            .build()
            .await?;

        TokenManager::start(
            auth,
            self.scopes,
            self.token_refresh.unwrap_or(Default::default()),
        )
        .await
    }
}

/// the response implments tonic's Into<tonic::Interceptor>
pub(crate) fn auth_interceptor(
    shared_token: Arc<ArcSwap<AccessToken>>,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static {
    move |mut req: Request<()>| {
        let bearer_token = format!("Bearer {}", shared_token.load().as_str());
        let token = MetadataValue::from_str(bearer_token.as_str()).unwrap();
        req.metadata_mut().insert("authorization", token);
        Ok(req)
    }
}
