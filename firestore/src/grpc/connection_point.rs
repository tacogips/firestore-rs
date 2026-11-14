pub struct GrpcConnectionPoint(pub &'static str, pub &'static str);
use lazy_static::lazy_static;

macro_rules! connect_points {
    ($($var_name:ident = $domain:expr), *,) => {
         lazy_static! {
             $(pub(crate) static ref $var_name : GrpcConnectionPoint = GrpcConnectionPoint(concat!("https://", $domain).as_ref(), $domain);) *
         }
    };
}

//ref. https://developers.google.com/apis-explorer
connect_points! {
    CLOUD_STORAGE = "storage.googleapis.com",
    FIRESTORE     = "firestore.googleapis.com",
    FIREBASE      = "firebase.googleapis.com",
    PUBSUB        = "pubsub.googleapis.com",
    RUN           = "run.googleapis.com",
}
