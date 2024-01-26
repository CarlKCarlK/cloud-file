cloud-files
==========

<!-- markdownlint-disable MD033 -->
[<img alt="github" src="https://img.shields.io/badge/github-cloud--files-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/CarlKCarlK/cloud-files)
[<img alt="crates.io" src="https://img.shields.io/crates/v/cloud-files.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/cloud-files)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-cloud--files-66c2a5?style=for-the-badge&labelColor=555555&logoColor=white&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">](https://docs.rs/cloud-files)
[<img alt="build status" src="https://img.shields.io/github/workflow/status/CarlKCarlK/cloud-files/CI/main?style=for-the-badge" height="20">](https://github.com/CarlKCarlK/cloud-files/actions?query=branch%3Amain)
<!-- markdownlint-enable MD033 -->

Simple reading of files in the cloud for Rust.

Highlights
----------

* HTTP, AWS S3, Azure, Google, or local
* Sequental or random access
* Simplifies use of the powerful [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate, focusing on a useful subset of its features.
* Access based on URLs and string-based options.
* Binary or text
* Used by genomics tool [BedReader](https://github.com/fastlmm/BedReader), which is used by other Rust and Python projects

Install
-------

```bash
cargo add cloud-files
```

Examples
--------

Find the size of a cloud file.

```rust
use cloud_files::{CloudFiles, EMPTY_OPTIONS};

# { use {cloud_files::CloudFilesError, tokio::runtime::Runtime}; // '#' needed for doctest
# Runtime::new().unwrap().block_on(async {
let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
let cloud_files = CloudFiles::new(url, EMPTY_OPTIONS)?;
let size = cloud_files.size().await?;
assert_eq!(size, 14_361);
# Ok::<(), Box<dyn std::error::Error>>(()) }).unwrap()};
```

Find the number of line in a cloud file.

```rust
use cloud_files::{CloudFiles,EMPTY_OPTIONS};
use futures_util::StreamExt;

# { use {cloud_files::CloudFilesError, tokio::runtime::Runtime};
# Runtime::new().unwrap().block_on(async {
let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
let cloud_files = CloudFiles::new(url, [("timeout", "30s")])?;
let mut stream = cloud_files.get().await?.into_stream();
let mut newline_count: usize = 0;
while let Some(bytes) = stream.next().await {
    let bytes = bytes?;
    let count = bytecount::count(&bytes, b'\n');
    newline_count += count;
}
assert_eq!(newline_count, 500);
# Ok::<(), Box<dyn std::error::Error>>(()) }).unwrap()};  // '#' needed for doctest
```

Project Links
-----

* [**Installation**](https://crates.io/crates/cloud-files)
* [**Documentation**](https://docs.rs/cloud-files/)
* [**Questions via email**](mailto://fastlmm-dev@python.org)
* [**Source code**](https://github.com/CarlKCarlK/cloud-files)
* [**Discussion**](https://github.com/CarlKCarlK/cloud-files/discussions/)
* [**Bug Reports**](https://github.com/CarlKCarlK/cloud-files/issues)
* [**Change Log**](https://github.com/CarlKCarlK/cloud-files/blob/main/CHANGELOG.md)
