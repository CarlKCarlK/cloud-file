use cloud_file::{CloudFile, CloudFileError, EMPTY_OPTIONS};
use futures_util::StreamExt;

async fn count_lines(cloud_file: &CloudFile) -> Result<usize, CloudFileError> {
    let mut stream = cloud_file.get().await?.into_stream();
    let mut newline_count: usize = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let count = bytecount::count(&bytes, b'\n');
        newline_count += count;
    }

    Ok(newline_count)
}

#[tokio::main]
async fn main() -> Result<(), CloudFileError> {
    let cloud_file = CloudFile::new(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
        EMPTY_OPTIONS,
    )?;
    let line_count = count_lines(&cloud_file).await?;
    println!("line_count: {}", line_count);
    Ok(())
}
