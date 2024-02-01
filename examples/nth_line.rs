use cloud_file::CloudFile;
use futures::StreamExt; // Enables `.next()` on streams.
use std::str::from_utf8;

async fn nth_line(cloud_file: &CloudFile, n: usize) -> Result<String, anyhow::Error> {
    // Each binary line_chunk contains one or more lines, that is, each chunk ends with a newline.
    let mut line_chunks = cloud_file.stream_line_chunks().await?;
    let mut index_iter = 0usize..;
    while let Some(line_chunk) = line_chunks.next().await {
        let line_chunk = line_chunk?;
        let lines = from_utf8(&line_chunk)?.lines();
        for line in lines {
            let index = index_iter.next().unwrap(); // safe because we know the iterator is infinite
            if index == n {
                return Ok(line.to_string());
            }
        }
    }
    Err(anyhow::anyhow!("Not enough lines in the file"))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    let n = 4;

    let cloud_file = CloudFile::new(url)?;
    let line = nth_line(&cloud_file, n).await?;
    println!("line at index {n}: {line}");
    Ok(())
}
