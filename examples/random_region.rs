use std::{collections::HashMap, ops::Range};

use cloud_file::{abs_path_to_url_string, CloudFile, EMPTY_OPTIONS};
use futures::pin_mut;
use futures_util::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // let cloud_file = CloudFile::new(
    //     "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
    //     EMPTY_OPTIONS,
    // )?;
    let file_name = r"C:\Users\carlk\OneDrive\programs\bed-sample-files\toydata.5chrom.fam";
    let url = abs_path_to_url_string(file_name)?;
    let cloud_file = CloudFile::new(url, EMPTY_OPTIONS)?;
    let size = cloud_file.size().await?;
    let seed = Some(0u64);
    let n = 1_000;
    let chunk_count = 100;
    let max_concurrent_requests = 10;

    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        StdRng::from_entropy()
    };
    let ranges: Vec<Range<usize>> = (0..n)
        .map(|_| rng.gen_range(0..size - 1)) // First map to generate the random start
        .map(|start| start..start + 2) // Second map to create the tuple range
        .collect();
    // println!("region_list: {:?}", ranges);

    let chunks = ranges.chunks(chunk_count);
    let iterator = chunks.map(|chunk| {
        let cloud_file = cloud_file.clone();
        async move { cloud_file.get_ranges(chunk).await }
    });

    let stream = futures_util::stream::iter(iterator).buffer_unordered(max_concurrent_requests);
    pin_mut!(stream);

    let mut bigram_counts = HashMap::new();
    while let Some(result) = stream.next().await {
        let bytes = result?;
        for b in bytes.iter() {
            let bigram = (b[0], b[1]);
            let count = bigram_counts.entry(bigram).or_insert(0);
            *count += 1;
        }
    }

    let mut bigram_count_vec: Vec<((u8, u8), usize)> = bigram_counts.into_iter().collect();
    bigram_count_vec.sort_by(|a, b| b.1.cmp(&a.1));
    for (bigram, count) in bigram_count_vec.into_iter().take(10) {
        let char1 = (bigram.0 as char).escape_default();
        let char2 = (bigram.1 as char).escape_default();
        println!("Bigram ('{}{}') occurs {} times", char1, char2, count);
    }
    Ok(())
}
