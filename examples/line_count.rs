use cloud_file::{CloudFile, CloudFileError};
use futures_util::StreamExt; // Enables `.next()` on streams.

async fn count_lines(cloud_file: &CloudFile) -> Result<usize, CloudFileError> {
    let mut chunks = cloud_file.stream_chunks().await?;
    let mut newline_count: usize = 0;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        newline_count += bytecount::count(&chunk, b'\n');
    }
    Ok(newline_count)
}

#[tokio::main]
async fn main() -> Result<(), CloudFileError> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    let options = [("timeout", "10s")];
    let cloud_file = CloudFile::new_with_options(url, options)?;
    let line_count = count_lines(&cloud_file).await?;
    println!("line_count: {line_count}");
    Ok(())
}
