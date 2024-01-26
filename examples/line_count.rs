use futures_util::StreamExt;
use object_path::{ObjectPath, ObjectPathError, EMPTY_OPTIONS};

async fn count_lines(object_path: &ObjectPath) -> Result<usize, ObjectPathError> {
    let mut stream = object_path.get().await?.into_stream();
    let mut newline_count: usize = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let count = bytecount::count(&bytes, b'\n');
        newline_count += count;
    }

    Ok(newline_count)
}

#[tokio::main]
async fn main() -> Result<(), ObjectPathError> {
    let object_path = ObjectPath::new(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
        EMPTY_OPTIONS,
    )?;
    let line_count = count_lines(&object_path).await?;
    println!("line_count: {}", line_count);
    Ok(())
}
