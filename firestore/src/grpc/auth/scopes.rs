use lazy_static::lazy_static;

pub type Scope = &'static str;

macro_rules! google_cloud_scope{
    ($($variable:ident = $val:expr),*, ) =>{
        lazy_static!{
            $(pub(crate) static ref $variable : Scope = concat!("https://www.googleapis.com/auth/", $val).as_ref();) *
        }
    }

}

// ref. https://developers.google.com/identity/protocols/oauth2/scopes?hl=en
google_cloud_scope! {
  CLOUD_PLATFORM           = "cloud-platform",
  CLOUD_PLATFORM_READ_ONLY = "cloud-platform.read-only",

  STORAGE_FULL             = "devstorage.full_control",
  STORAGE_READ_ONLY        = "devstorage.read_only",
  STORAGE_READ_WRITE       = "devstorage.read_write",

  PUBSUB                   = "pubsub",

  COMPUTE                  = "compute",
  DATASTORE                = "datastore",
  FIREBASE                 = "firebase",

  LOGGING_ADMIN            = "logging.admin",
  LOGGING_READ             = "logging.read",
  LOGGING_WRITE            = "logging.write",
}
