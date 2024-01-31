// For comparison, this is a version file aws_file size that uses structs rather than URL strings.
use cloud_file::CloudFile;
use rusoto_credential::{CredentialsError, ProfileProvider, ProvideAwsCredentials};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // get credentials from ~/.aws/credentials
    let credentials = if let Ok(provider) = ProfileProvider::new() {
        provider.credentials().await
    } else {
        Err(CredentialsError::new("No credentials found"))
    };

    let Ok(credentials) = credentials else {
        eprintln!("Skipping example because no AWS credentials found");
        return Ok(());
    };

    use object_store::{aws::AmazonS3Builder, path::Path as StorePath};

    let s3 = AmazonS3Builder::new()
        .with_region("us-west-2")
        .with_bucket_name("bedreader")
        .with_access_key_id(credentials.aws_access_key_id())
        .with_secret_access_key(credentials.aws_secret_access_key())
        .build()?;
    let store_path = StorePath::parse("v1/toydata.5chrom.bed")?;
    let cloud_file = CloudFile::from_structs(s3, store_path);

    assert_eq!(cloud_file.read_file_size().await?, 1_250_003);
    Ok(())
}
