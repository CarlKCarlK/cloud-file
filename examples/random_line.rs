use anyhow::anyhow;
use futures::pin_mut;
use futures_util::StreamExt;
use object_path::{ObjectPath, EMPTY_OPTIONS};
use object_store::delimited::newline_delimited_stream;
use object_store::ObjectStore;
use rand::{rngs::StdRng, Rng, SeedableRng};

async fn random_line<TObjectStore>(
    object_path: &ObjectPath<TObjectStore>,
    seed: Option<u64>,
) -> Result<String, anyhow::Error>
where
    TObjectStore: ObjectStore,
{
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        StdRng::from_entropy()
    };
    let mut selected_line = None;

    let stream = object_path.get().await?.into_stream();
    let line_chunk_stream = newline_delimited_stream(stream);
    pin_mut!(line_chunk_stream);

    let mut index_iter = 0..;
    while let Some(line_chunk) = line_chunk_stream.next().await {
        let line_chunk = line_chunk?;
        let lines = std::str::from_utf8(&line_chunk)?.split_terminator('\n');

        for line in lines {
            let index = index_iter.next().unwrap();
            // Reservoir sampling: replace the selected line with probability 1/(index+1)
            if rng.gen_range(0..=index) == 0 {
                selected_line = Some(line.to_string());
            }
        }
    }

    selected_line.ok_or_else(|| anyhow!("No lines found in the file"))
}

// cmk how can you test in memory with a URL?
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let object_path = ObjectPath::from_url(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
        EMPTY_OPTIONS,
    )?;
    let line = random_line(&object_path, None).await?;
    println!("random line: {line}");
    Ok(())
}
