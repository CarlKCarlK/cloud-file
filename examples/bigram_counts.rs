#[cfg(not(target_pointer_width = "64"))]
compile_error!("This code requires a 64-bit target architecture.");

use cloud_file::CloudFile;
use futures::pin_mut;
use futures_util::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{cmp::max, collections::HashMap, ops::Range};

async fn count_bigrams(
    cloud_file: CloudFile,
    sample_count: usize,
    seed: Option<u64>,
    max_concurrent_requests: usize,
    max_chunk_bytes: usize,
) -> Result<(), anyhow::Error> {
    // Create a random number generator
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        StdRng::from_entropy()
    };

    // Find the document size
    let file_size = cloud_file.read_file_size().await?;

    // Randomly choose the two-byte ranges to sample
    let range_samples: Vec<Range<usize>> = (0..sample_count)
        .map(|_| rng.gen_range(0..file_size - 1))
        .map(|start| start..start + 2)
        .collect();

    // println!("{:?}", range_samples);

    // Divide the ranges into chunks respecting the max_chunk_bytes limit
    const BYTES_PER_BIGRAM: usize = 2;
    let chunk_count = max(1, max_chunk_bytes / BYTES_PER_BIGRAM);
    let range_chunks = range_samples.chunks(chunk_count);

    // Create an iterator of future work
    let work_chunks_iterator = range_chunks.map(|chunk| {
        let cloud_file = cloud_file.clone(); // by design, clone is cheap
        async move { cloud_file.read_ranges(chunk).await }
    });

    // Create a stream of futures to run out-of-order and with constrained concurrency.
    let work_chunks_stream =
        futures_util::stream::iter(work_chunks_iterator).buffer_unordered(max_concurrent_requests);
    pin_mut!(work_chunks_stream);

    // Run the futures and, as result bytes come in, tabulate.
    let mut bigram_counts = HashMap::new();
    while let Some(result) = work_chunks_stream.next().await {
        let bytes_vec = result?;
        for bytes in bytes_vec.iter() {
            let bigram = (bytes[0], bytes[1]);
            let count = bigram_counts.entry(bigram).or_insert(0);
            *count += 1;
        }
    }

    // Sort the bigrams by count and print the top 10
    let mut bigram_count_vec: Vec<(_, usize)> = bigram_counts.into_iter().collect();
    bigram_count_vec.sort_by(|a, b| b.1.cmp(&a.1));
    for (bigram, count) in bigram_count_vec.into_iter().take(10) {
        let char0 = (bigram.0 as char).escape_default();
        let char1 = (bigram.1 as char).escape_default();
        println!("Bigram ('{}{}') occurs {} times", char0, char1, count);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = "https://www.gutenberg.org/cache/epub/100/pg100.txt";
    let options = [("timeout", "30s")];
    let cloud_file = CloudFile::new_with_options(url, options)?;

    let seed = Some(0u64);
    let sample_count = 10_000;
    let max_chunk_bytes = 750; // 8_000_000 is a good default when chunks are bigger.
    let max_concurrent_requests = 10; // 10 is a good default

    count_bigrams(
        cloud_file,
        sample_count,
        seed,
        max_concurrent_requests,
        max_chunk_bytes,
    )
    .await?;

    Ok(())
}
