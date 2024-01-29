use anyhow::anyhow;
use cloud_file::CloudFile;
use futures::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::str::from_utf8;

async fn random_line(cloud_file: &CloudFile, seed: Option<u64>) -> Result<String, anyhow::Error> {
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        StdRng::from_entropy()
    };

    let mut selected_line = None;

    // Each binary line_chunk contains one or more lines, that is, each chunk ends with a newline.
    let mut line_chunks = cloud_file.line_chunks().await?;
    let mut index_iter = 0..;
    while let Some(line_chunk) = line_chunks.next().await {
        let line_chunk = line_chunk?;
        let lines = from_utf8(&line_chunk)?.split_terminator('\n');
        for line in lines {
            let index = index_iter.next().unwrap(); // safe because we know the iterator is infinite

            // For an explanation of this one-pass random-line algorithm, see
            // https://towardsdatascience.com/interview-question-select-a-random-line-from-a-file-in-rust-c0a8cddcddfb
            if rng.gen_range(0..=index) == 0 {
                selected_line = Some(line.to_string());
            }
        }
    }

    selected_line.ok_or_else(|| anyhow!("No lines found in the file"))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    let cloud_file = CloudFile::new(url)?;
    let line = random_line(&cloud_file, None).await?;
    println!("random line: {line}");
    Ok(())
}
