use std::{any, error::Error, ops::Range};

use anyhow::anyhow;
use bytes::Bytes;
use futures::pin_mut;
use futures_util::StreamExt;
use object_path::{abs_path_to_url_string, ObjectPath, EMPTY_OPTIONS};
use object_store::delimited::newline_delimited_stream;
use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};

fn try_nth<I, T, E>(mut iterator: I, n: usize) -> Result<Option<T>, E>
where
    I: Iterator<Item = Result<T, E>>,
{
    for _ in 0..n {
        if iterator.next().transpose()?.is_none() {
            return Ok(None);
        }
    }
    iterator.next().transpose()
}

fn random_regions<R>(rng: &mut R, region_length: usize) -> Vec<Range<usize>>
where
    R: RngCore,
{
    let mut region_list = vec![];
    let mut index1: usize = 1;
    loop {
        let end = (index1 - 1).saturating_add(region_length);
        region_list.push(index1 - 1..end);
        let r: f64 = rng.gen();
        let offset = ((r * (index1 as f64) / (1.0 - r)).ceil() as usize).max(1);
        // // cmk000
        // if index1 > 1_000_000 {
        //     break;
        // }
        if let Some(new_index) = index1.checked_add(offset) {
            index1 = new_index;
        } else {
            break; // Break if overflow would occur
        }
    }
    region_list
}

async fn random_region(
    object_path: &ObjectPath,
    seed: Option<u64>,
    region_length: usize,
) -> Result<Bytes, anyhow::Error> {
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        StdRng::from_entropy()
    };

    let region_list = random_regions(&mut rng, region_length);
    println!("region_list: {:?}", region_list);
    let vec_bytes = object_path.get_ranges(&region_list).await?;
    println!("return len: {:?}", vec_bytes.len());

    todo!("cmk")
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // let object_path = ObjectPath::new(
    //     "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam",
    //     EMPTY_OPTIONS,
    // )?;
    let file_name = r"C:\Users\carlk\OneDrive\programs\bed-sample-files\toydata.5chrom.fam";
    let url = abs_path_to_url_string(file_name)?;
    let object_path = ObjectPath::new(url, EMPTY_OPTIONS)?;

    let line = random_region(&object_path, None, 10).await?;
    // println!("random line: {line}");
    Ok(())
}
