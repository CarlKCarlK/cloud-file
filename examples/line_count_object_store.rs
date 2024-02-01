// For comparison, here is a version of line_counts.rs that uses the object_store directly
// rather than the cloud-file wrapper crate.
use futures_util::StreamExt; // Enables `.next()` on streams.
pub use object_store::path::Path as StorePath;
use object_store::{parse_url_opts, ObjectStore};
use std::sync::Arc;
use url::Url;

async fn count_lines(
    object_store: &Arc<Box<dyn ObjectStore>>,
    store_path: StorePath,
) -> Result<usize, anyhow::Error> {
    let mut chunks = object_store.get(&store_path).await?.into_stream();
    let mut newline_count: usize = 0;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        newline_count += bytecount::count(&chunk, b'\n');
    }
    Ok(newline_count)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    let options = [("timeout", "10s")];

    let url = Url::parse(url)?;
    let (object_store, store_path) = parse_url_opts(&url, options)?;
    let object_store = Arc::new(object_store); // enables cloning and borrowing
    let line_count = count_lines(&object_store, store_path).await?;
    println!("line_count: {line_count}");
    Ok(())
}
