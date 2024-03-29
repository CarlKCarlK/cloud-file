cloud-file
==========

<!-- markdownlint-disable MD033 -->
[<img alt="github" src="https://img.shields.io/badge/github-cloud--file-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/CarlKCarlK/cloud-file)
[<img alt="crates.io" src="https://img.shields.io/crates/v/cloud-file.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/cloud-file)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-cloud--file-66c2a5?style=for-the-badge&labelColor=555555&logoColor=white&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">](https://docs.rs/cloud-file)
[<img alt="build status" src="https://img.shields.io/github/workflow/status/CarlKCarlK/cloud-file/CI/main?style=for-the-badge" height="20">](https://github.com/CarlKCarlK/cloud-file/actions?query=branch%3Amain)
<!-- markdownlint-enable MD033 -->

Simple reading of cloud files in Rust

Highlights
----------

* HTTP, AWS S3, Azure, Google, or local
* Sequential or random access
* Simplifies use of the powerful [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate, focusing on a useful subset of its features
* Access files via URLs and string-based options
* Read binary or text
* Fully async
* Used by genomics crate [BedReader](https://github.com/fastlmm/BedReader), which is used by other Rust and Python projects
* Also see [Nine Rules for Accessing Cloud Files from Your Rust Code
Practical Lessons from Upgrading Bed-Reader, a Bioinformatics Library](https://medium.com/towards-data-science/nine-rules-for-accessing-cloud-files-from-your-rust-code-d456c1e2ceb4) in *Towards Data Science*.

Install
-------

```bash
cargo add cloud-file
```

Examples
--------

Find the size of a cloud file.

```rust
use cloud_file::CloudFile;
# Runtime::new().unwrap().block_on(async {  // '#' needed for doctest

let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
let cloud_file = CloudFile::new(url)?;
let file_size = cloud_file.read_file_size().await?;
assert_eq!(file_size, 14_361);
# Ok::<(), Box<dyn std::error::Error>>(()) }).unwrap();
# use {cloud_file::CloudFileError, tokio::runtime::Runtime};
```

Find the number of lines in a cloud file.

```rust
use cloud_file::CloudFile;
use futures::StreamExt; // Enables `.next()` on streams.
# Runtime::new().unwrap().block_on(async { // '#' needed for doctest

let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
let cloud_file = CloudFile::new_with_options(url, [("timeout", "30s")])?;
let mut chunks = cloud_file.stream_chunks().await?;
let mut newline_count: usize = 0;
while let Some(chunk) = chunks.next().await {
    let chunk = chunk?;
    newline_count += bytecount::count(&chunk, b'\n');
}
assert_eq!(newline_count, 500);
# Ok::<(), Box<dyn std::error::Error>>(()) }).unwrap();
# use {cloud_file::CloudFileError, tokio::runtime::Runtime};   
```

More examples
--------------

| Example                                       | Demonstrates                                  |
|-----------------------------------------------|-----------------------------------------------|
| [`line_count`](https://github.com/CarlKCarlK/cloud-file/blob/main/examples/line_count.rs)     | Read a file as binary chunks.  |
| [`nth_line`](https://github.com/CarlKCarlK/cloud-file/blob/main/examples/nth_line.rs)   | Read a file as text lines.        |
| [`bigram_counts`](https://github.com/CarlKCarlK/cloud-file/blob/main/examples/bigram_counts.rs) | Read random regions of a file, without regard to order.   |
| [`aws_file_size`](https://github.com/CarlKCarlK/cloud-file/blob/main/examples/aws_file_size.rs) | Find the size of a file on AWS.   |

Project Links
-----

* [**Installation**](https://crates.io/crates/cloud-file)
* [**Documentation**](https://docs.rs/cloud-file/)
* [**Discussion and Questions**](https://github.com/CarlKCarlK/cloud-file/discussions/)
* [**Source code**](https://github.com/CarlKCarlK/cloud-file)
* [**Bug Reports**](https://github.com/CarlKCarlK/cloud-file/issues)
* [**Change Log**](https://github.com/CarlKCarlK/cloud-file/blob/main/CHANGELOG.md)
