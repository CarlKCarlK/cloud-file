use cloud_files::{CloudFiles, CloudFilesError, EMPTY_OPTIONS};
use futures_util::StreamExt;

async fn count_lines(cloud_files: &CloudFiles) -> Result<usize, CloudFilesError> {
    let mut stream = cloud_files.get().await?.into_stream();
    let mut newline_count: usize = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let count = bytecount::count(&bytes, b'\n');
        newline_count += count;
    }

    Ok(newline_count)
}

#[tokio::main]
async fn main() -> Result<(), CloudFilesError> {
    let cloud_files = CloudFiles::new(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
        EMPTY_OPTIONS,
    )?;
    let line_count = count_lines(&cloud_files).await?;
    println!("line_count: {}", line_count);
    Ok(())
}
