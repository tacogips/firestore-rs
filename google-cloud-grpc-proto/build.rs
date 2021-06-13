fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(false).compile(
        &[
            "proto/google/storage/v1/storage.proto",
            "proto/google/storagetransfer/v1/transfer.proto",
            "proto/google/firestore/admin/v1/firestore_admin.proto",
            "proto/google/firestore/admin/v1beta1/firestore_admin.proto",
            "proto/google/firestore/admin/v1beta2/firestore_admin.proto",
            "proto/google/firestore/v1/firestore.proto",
            "proto/google/firestore/v1beta1/firestore.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
