pub mod firestore {

    pub mod admin {
        pub mod v1 {
            tonic::include_proto!("google.firestore.admin.v1");
        }

        pub mod v1beta1 {
            tonic::include_proto!("google.firestore.admin.v1beta1");
        }

        pub mod v1beta2 {
            tonic::include_proto!("google.firestore.admin.v1beta2");
        }
    }
    pub mod v1 {
        tonic::include_proto!("google.firestore.v1");
    }
    pub mod v1beta1 {
        tonic::include_proto!("google.firestore.v1beta1");
    }
}

pub mod longrunning {
    tonic::include_proto!("google.longrunning");
}

pub mod rpc {
    tonic::include_proto!("google.rpc");
}
pub mod r#type {
    tonic::include_proto!("google.r#type");
}

pub use prost_types;
pub use tonic;
