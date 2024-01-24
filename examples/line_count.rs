use futures_util::StreamExt;
use object_path::{ObjectPath, EMPTY_OPTIONS};
use object_store::ObjectStore;

async fn count_lines<TObjectStore>(
    object_path: &ObjectPath<TObjectStore>,
) -> Result<usize, anyhow::Error>
where
    TObjectStore: ObjectStore,
{
    let mut stream = object_path.get().await?.into_stream();
    let mut newline_count: usize = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let count = bytecount::count(&bytes, b'\n');
        newline_count += count;
    }

    Ok(newline_count)
}

// cmk how can you test in memory with a URL?
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let object_path = ObjectPath::from_url(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
        EMPTY_OPTIONS,
    )?;
    let line_count = count_lines(&object_path).await?;
    println!("line_count: {}", line_count);
    Ok(())
}
