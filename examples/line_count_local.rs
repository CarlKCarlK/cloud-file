// For comparison, here is a local version of line_count.rs
use std::fs::File;
use std::io::{self, BufRead, BufReader};

fn main() -> io::Result<()> {
    let path = "examples/line_count_local.rs";
    let reader = BufReader::new(File::open(path)?);
    let mut line_count = 0;
    for line in reader.lines() {
        let _line = line?;
        line_count += 1;
    }
    println!("line_count: {line_count}");
    Ok(())
}
