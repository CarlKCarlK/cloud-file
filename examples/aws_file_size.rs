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

    let url = "s3://bedreader/v1/toydata.5chrom.bed";
    let options = [
        ("aws_region", "us-west-2"),
        ("aws_access_key_id", credentials.aws_access_key_id()),
        ("aws_secret_access_key", credentials.aws_secret_access_key()),
    ];

    let cloud_file = CloudFile::new_with_options(url, options)?;
    assert_eq!(cloud_file.size().await?, 1_250_003);
    Ok(())
}
