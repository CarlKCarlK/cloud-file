#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_panics_doc, // LATER: add panics docs
    clippy::missing_errors_doc, // LATER: add errors docs
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless
)]
#![doc = include_str!("../README.md")]
//! ## Main Functions
//!
//! | Function | Description |
//! | -------- | ----------- |
//! | [`CloudFile::new`](struct.CloudFile.html#method.new) | Use a URL to specify a cloud file for reading. |
//! | [`CloudFile::new_with_options`](struct.CloudFile.html#method.new_with_options) | Use a URL and options to specify a cloud file for reading. |
//!
//! ## URLs
//!
//! | Cloud Service | Example |
//! | ------------- | ------- |
//! | HTTP          | `https://www.gutenberg.org/cache/epub/100/pg100.txt` |
//! | local file    | `file:///M:/data%20files/small.bed` |
//! | AWS S3        | `s3://bedreader/v1/toydata.5chrom.bed` |
//!
//! Note: For local files, use the [`abs_path_to_url_string`](fn.abs_path_to_url_string.html) function to properly encode into a URL.
//! 
//! ## Options
//!
//! | Cloud Service | Details | Example |
//! | -------- | ------- | ----------- |
//! | HTTP | [`ClientConfigKey`](https://docs.rs/object_store/latest/object_store/enum.ClientConfigKey.html#variant.Timeout) | `[("timeout", "30s")]` |
//! | local file | *none* | |
//! | AWS S3 | [`AmazonS3ConfigKey`](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html) | `[("aws_region", "us-west-2"), ("aws_access_key_id",` ...`), ("aws_secret_access_key",` ...`)]` |
//! | Azure | [`AzureConfigKey`](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html) |  |
//! | Google | [`GoogleConfigKey`](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html) |  |
//! 
//!
//! ## High-Level [`CloudFile`](struct.CloudFile.html) Methods
//!
//! | Method | Description |
//! | -------- | ----------- |
//! | [`open`](struct.CloudFile.html#method.open) | Open the file to read as a stream of bytes. |
//! | [`line_chunks`](struct.CloudFile.html#method.line_chunks) | Opens a file to read binary chunks of one or more lines, that is, each chunk ends with a newline. |
//! | [`bytes`](struct.CloudFile.html#method.bytes) | Read the whole file into an in-memory [`Bytes`](https://docs.rs/bytes/latest/bytes/struct.Bytes.html). |
//! | [`range_and_file_size`](struct.CloudFile.html#method.range_and_file_size) | Retrieve a range of bytes and the file's total size. |
//! | [`ranges`](struct.CloudFile.html#method.ranges) | Return the bytes found in the specified ranges. |
//! | [`clone`](struct.CloudFile.html#method.clone) | Clone the [`CloudFile`](struct.CloudFile.html). By design, this is efficient. |
//! | [`size`](struct.CloudFile.html#method.size) | Return the size of a file stored in the cloud. |
//! | [`line_count`](struct.CloudFile.html#method.line_count) | Count the lines in a file stored in the cloud. |
//! | [`set_extension`](struct.CloudFile.html#method.set_extension) | Change the [`CloudFile`]'s extension (in place). |
//! 
//! ## Low-Level [`CloudFile`](struct.CloudFile.html) Methods
//! 
//! | Method | Description |
//! | -------- | ----------- |
//! | [`get`](struct.CloudFile.html#method.get) | Call the [`object_store`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#method.get) crate's `get` method. |
//! | [`get_opts`](struct.CloudFile.html#method.get_opts) | Call the [`object_store`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#method.get_opts) crate's `get_opts` method. |
//! 
//! ## Lowest-Level [`CloudFile`](struct.CloudFile.html) Methods
//! 
//! You can call any method from the [`object_store`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) crate. For example, here we
//! use [`head`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#tymethod.head) to get the metadata for a file and the last_modified time.
//! 
//! ```
//! use cloud_file::CloudFile;
//!
//! # Runtime::new().unwrap().block_on(async {
//! let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
//! let cloud_file = CloudFile::new(url)?;
//! let meta = cloud_file.cloud_service.head(&cloud_file.store_path).await?;
//! let last_modified = meta.last_modified;
//! println!("last_modified: {}", last_modified);
//! assert_eq!(meta.size, 303);
//! # Ok::<(), CloudFileError>(())}).unwrap();
//! # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
//! ```

#[cfg(not(target_pointer_width = "64"))]
compile_error!("This code requires a 64-bit target architecture.");

use bytes::Bytes;
use object_store::delimited::newline_delimited_stream;
use core::fmt;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::http::HttpBuilder;
#[doc(no_inline)]
pub use object_store::path::Path as StorePath;
use object_store::{GetOptions, GetRange, GetResult, ObjectStore};
use std::ops::{Deref, Range};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

#[derive(Debug)]
/// The main struct representing the location of a file in the cloud.
///
/// It is constructed with [`CloudFile::new`](struct.CloudFile.html#method.new). It is, by design, cheap to clone.
///
/// Internally, it stores two pieces of information: the file's cloud service and the path to the file on that service.
///
/// # Examples
///
/// ```
/// use cloud_file::CloudFile;
///
/// # Runtime::new().unwrap().block_on(async {
/// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
/// let cloud_file = CloudFile::new(url)?;
/// assert_eq!(cloud_file.size().await?, 303);
/// # Ok::<(), CloudFileError>(())}).unwrap();
/// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
/// ```
pub struct CloudFile {
    /// A cloud service, for example, Http, AWS S3, Azure, the local file system, etc.
    /// Under the covers, it is an `Arc`-wrapped [`DynObjectStore`](struct.DynObjectStore.html).
    /// The `DynObjectStore`, in turn, holds an [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) from the
    /// powerful [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate.
    pub cloud_service: Arc<DynObjectStore>,
    /// A path to a file on the cloud service.
    /// Under the covers, `StorePath` is an alias for a [`Path`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html)
    /// in the [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate.
    pub store_path: StorePath,
}

impl Clone for CloudFile {
    fn clone(&self) -> Self {
        CloudFile {
            cloud_service: self.cloud_service.clone(),
            store_path: self.store_path.clone(),
        }
    }
}

/// An empty set of cloud options
pub const EMPTY_OPTIONS: [(&str, String); 0] = [];

impl CloudFile {
    /// Create a new [`CloudFile`] from a URL string.
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(url)?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub fn new(location: impl AsRef<str>) -> Result<CloudFile, CloudFileError> {
        let location = location.as_ref();
        let url = Url::parse(location)
            .map_err(|e| CloudFileError::CannotParseUrl(location.to_string(), e.to_string()))?;

        let (object_store, store_path): (DynObjectStore, StorePath) =
            parse_url_opts_work_around(&url, EMPTY_OPTIONS)?;
        let cloud_file = CloudFile {
            cloud_service: Arc::new(object_store),
            store_path,
        };
        Ok(cloud_file)
    }

    /// Create a new [`CloudFile`] from a URL string and options.
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new_with_options(url, [("timeout", "30s")])?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub fn new_with_options<I, K, V>(
        location: impl AsRef<str>,
        options: I,
    ) -> Result<CloudFile, CloudFileError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let location = location.as_ref();
        let url = Url::parse(location)
            .map_err(|e| CloudFileError::CannotParseUrl(location.to_string(), e.to_string()))?;

        let (object_store, store_path): (DynObjectStore, StorePath) =
            parse_url_opts_work_around(&url, options)?;
        let cloud_file = CloudFile {
            cloud_service: Arc::new(object_store),
            store_path,
        };
        Ok(cloud_file)
    }

    /// Count the lines in a file stored in the cloud.
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.fam";
    /// let cloud_file = CloudFile::new(url)?;
    /// assert_eq!(cloud_file.line_count().await?, 10);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn line_count(&self) -> Result<usize, CloudFileError> {
        let stream = self.open().await?;

        let newline_count = stream
            .try_fold(0, |acc, bytes| async move {
                let count = bytecount::count(&bytes, b'\n');
                Ok(acc + count) // Accumulate the count
            })
            .await
            .map_err(CloudFileError::ObjectStoreError)?;
        Ok(newline_count)
    }

    /// Return the size of a file stored in the cloud.
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(url)?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn size(&self) -> Result<usize, CloudFileError> {
        let meta = self.cloud_service.head(&self.store_path).await?;
        Ok(meta.size)
    }

    /// Return the bytes found in the specified ranges.
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bim";
    /// let cloud_file = CloudFile::new(url)?;
    /// let byte_vec = cloud_file.ranges(&[0..10, 1000..1010]).await?;
    /// assert_eq!(byte_vec.len(), 2);
    /// assert_eq!(byte_vec[0].as_ref(), b"1\t1:1:A:C\t");
    /// assert_eq!(byte_vec[1].as_ref(), b":A:C\t0.0\t4");
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn ranges(&self, ranges: &[Range<usize>]) -> Result<Vec<Bytes>, CloudFileError> {
        Ok(self
            .cloud_service
            .get_ranges(&self.store_path, ranges)
            .await?)
    }

    /// Call the [`object_store`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#method.get_opts) crate's `get_opts` method.
    /// 
    /// You can, for example, in one call retrieve a range of bytes from the file and the file's metadata. The
    /// result is a [`GetResult`](https://docs.rs/object_store/latest/object_store/struct.GetResult.html).
    ///
    /// # Example
    ///
    /// In one call, read the first three bytes of a genomic data file and get
    /// the size of the file. Check that the file starts with the expected file signature.
    /// ```
    /// use cloud_file::CloudFile;
    /// use object_store::{GetRange, GetOptions};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(url)?;
    /// let get_options = GetOptions {
    ///     range: Some(GetRange::Bounded(0..3)),
    ///     ..Default::default()
    /// };
    /// let get_result = cloud_file.get_opts(get_options).await?;
    /// let size: usize = get_result.meta.size;
    /// let bytes = get_result
    ///     .bytes()
    ///     .await?;
    /// assert_eq!(bytes[0], 0x6c);
    /// assert_eq!(bytes[1], 0x1b);
    /// assert_eq!(bytes[2], 0x01);
    /// assert_eq!(size, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn get_opts(&self, get_options: GetOptions) -> Result<GetResult, CloudFileError> {
        Ok(self
            .cloud_service
            .get_opts(&self.store_path, get_options)
            .await?)
    }

    /// Retrieve a range of bytes and the file's total size.
    ///
    /// # Example
    ///
    /// In one call, read the first three bytes of a genomic data file and get
    /// the size of the file. Check that the file starts with the expected file signature.
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(url)?;
    /// let (bytes, size) = cloud_file.range_and_file_size(0..3).await?;
    /// assert_eq!(bytes[0], 0x6c);
    /// assert_eq!(bytes[1], 0x1b);
    /// assert_eq!(bytes[2], 0x01);
    /// assert_eq!(size, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn range_and_file_size(
        &self,
        range: Range<usize>,
    ) -> Result<(Bytes, usize), CloudFileError> {
        let get_options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..Default::default()
        };
        let get_result = self
            .cloud_service
            .get_opts(&self.store_path, get_options)
            .await?;
        let size: usize = get_result.meta.size;
        let bytes = get_result
            .bytes()
            .await
            .map_err(CloudFileError::ObjectStoreError)?;
        Ok((bytes, size))
    }

    /// Call the [`object_store`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#method.get) crate's `get` method.
    ///
    /// The result is a [`GetResult`](https://docs.rs/object_store/latest/object_store/struct.GetResult.html) which can,
    /// for example, be converted into a stream of bytes.
    ///
    /// # Example
    ///
    /// Do a 'get', turn result into a stream, then scan all the bytes of the
    /// file for the newline character.
    ///
    /// ```rust
    /// use cloud_file::CloudFile;
    /// use futures_util::StreamExt;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    /// let cloud_file = CloudFile::new_with_options(url, [("timeout", "30s")])?;
    /// let mut stream = cloud_file.get().await?.into_stream();
    /// let mut newline_count: usize = 0;
    /// while let Some(bytes) = stream.next().await {
    ///     let bytes = bytes?;
    ///     newline_count += bytecount::count(&bytes, b'\n');
    ///     }
    /// assert_eq!(newline_count, 500);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn get(&self) -> Result<GetResult, CloudFileError> {
        Ok(self.cloud_service.get(&self.store_path).await?)
    }

    /// Read the whole file into an in-memory [`Bytes`](https://docs.rs/bytes/latest/bytes/struct.Bytes.html).
    ///
    /// # Example
    ///
    /// Read the whole file, then scan all the bytes of the
    /// for the newline character.
    ///
    /// ```rust
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    /// let cloud_file = CloudFile::new_with_options(url, [("timeout", "30s")])?;
    /// let bytes = cloud_file.bytes().await?;
    /// let newline_count = bytecount::count(&bytes, b'\n');
    /// assert_eq!(newline_count, 500);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn bytes(&self) -> Result<Bytes, CloudFileError> {
        let bytes = self
            .cloud_service
            .get(&self.store_path)
            .await?
            .bytes()
            .await?;
        Ok(bytes)
    }

    /// Open the file to read as a stream of bytes.
    ///
    /// # Example
    ///
    /// Open the file as a stream of bytes, then scan all the bytes
    /// for the newline character.
    ///
    /// ```rust
    /// use cloud_file::CloudFile;
    /// use futures::StreamExt; // Enables `.next()` on streams.
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    /// let cloud_file = CloudFile::new_with_options(url, [("timeout", "30s")])?;
    /// let mut stream = cloud_file.open().await?;
    /// let mut newline_count: usize = 0;
    /// while let Some(bytes) = stream.next().await {
    ///     let bytes = bytes?;
    ///     newline_count += bytecount::count(&bytes, b'\n');
    ///     }
    /// assert_eq!(newline_count, 500);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn open(
        &self,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>, CloudFileError> {
        let stream = self
            .cloud_service
            .get(&self.store_path)
            .await?
            .into_stream();
        Ok(stream)
    }

    ///  Open a file to read binary chunks of one or more lines, that is, each chunk ends with a newline.
    ///
    /// # Example
    ///
    /// Return the 12th line of a file.
    ///
    /// ```rust
    /// use cloud_file::CloudFile;
    /// use futures::StreamExt; // Enables `.next()` on streams.
    /// use std::str::from_utf8;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    /// let goal_index = 12;
    ///
    /// let cloud_file = CloudFile::new(url)?;
    /// let mut line_chunks = cloud_file.line_chunks().await?;
    /// let mut index_iter = 0..;
    /// let mut goal_line = None;
    /// 'outer_loop: while let Some(line_chunk) = line_chunks.next().await {
    ///     let line_chunk = line_chunk?;
    ///     let lines = from_utf8(&line_chunk)?.split_terminator('\n');
    ///     for line in lines {
    ///         let index = index_iter.next().unwrap(); // Safe because the iterator is infinite
    ///         if index == goal_index {
    ///             goal_line = Some(line.to_string());
    ///             break 'outer_loop;
    ///         }
    ///     }
    /// }
    /// assert_eq!(goal_line, Some("per12 per12 0 0 2 -0.0382707".to_string()));
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    ///
    pub async fn line_chunks(
        &self,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>, CloudFileError> {
        let stream = self.open().await?;
        let line_chunks = newline_delimited_stream(stream);
        Ok(Box::pin(line_chunks))
    }


    /// Change the [`CloudFile`]'s extension (in place).
    ///
    /// It removes the current extension, if any.
    /// It appends the given extension, if any.
    ///
    /// The method is in-place rather than functional to make it consistent with
    /// [`std::path::PathBuf::set_extension`](https://doc.rust-lang.org/stable/std/path/struct.PathBuf.html#method.set_extension).
    ///
    /// # Example
    /// ```
    /// use cloud_file::CloudFile;
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let mut cloud_file = CloudFile::new(url)?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// cloud_file.set_extension("fam")?;
    /// assert_eq!(cloud_file.size().await?, 130);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub fn set_extension(&mut self, extension: &str) -> Result<(), CloudFileError> {
        let mut path_str = self.store_path.to_string();

        // Find the last dot in the object path
        if let Some(dot_index) = path_str.rfind('.') {
            // Remove the current extension
            path_str.truncate(dot_index);
        }

        if !extension.is_empty() {
            // Append the new extension
            path_str.push('.');
            path_str.push_str(extension);
        }

        // Parse the string back to StorePath
        self.store_path = StorePath::parse(&path_str)?;
        Ok(())
    }
}

#[allow(clippy::match_bool)]
fn parse_work_around(url: &Url) -> Result<(bool, StorePath), object_store::Error> {
    let strip_bucket = || Some(url.path().strip_prefix('/')?.split_once('/')?.1);

    let (scheme, path) = match (url.scheme(), url.host_str()) {
        ("http", Some(_)) => (true, url.path()),
        ("https", Some(host)) => {
            if host.ends_with("dfs.core.windows.net")
                || host.ends_with("blob.core.windows.net")
                || host.ends_with("dfs.fabric.microsoft.com")
                || host.ends_with("blob.fabric.microsoft.com")
            {
                (false, url.path())
            } else if host.ends_with("amazonaws.com") {
                match host.starts_with("s3") {
                    true => (false, strip_bucket().unwrap_or_default()),
                    false => (false, url.path()),
                }
            } else if host.ends_with("r2.cloudflarestorage.com") {
                (false, strip_bucket().unwrap_or_default())
            } else {
                (true, url.path())
            }
        }
        _ => (false, url.path()),
    };

    Ok((scheme, StorePath::from_url_path(path)?))
}

// LATER when https://github.com/apache/arrow-rs/issues/5310 gets fixed, can remove work around
fn parse_url_opts_work_around<I, K, V>(
    url: &Url,
    options: I,
) -> Result<(DynObjectStore, StorePath), object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (is_http, path) = parse_work_around(url)?;
    if is_http {
        let url = &url[..url::Position::BeforePath];
        let path = StorePath::parse(path)?;
        let builder = options.into_iter().fold(
            <HttpBuilder>::new().with_url(url),
            |builder, (key, value)| match key.as_ref().parse() {
                Ok(k) => builder.with_config(k, value),
                Err(_) => builder,
            },
        );
        let store = DynObjectStore::new(builder.build()?);
        Ok((store, path))
    } else {
        let (store, path) = object_store::parse_url_opts(url, options)?;
        Ok((DynObjectStore(store), path))
    }
}

impl fmt::Display for CloudFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CloudFile: {:?}", self.store_path)
    }
}

/// Wraps `Box<dyn ObjectStore>` for easier usage. An [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html), from the
/// powerful [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate, represents a cloud service.
#[derive(Debug)]
pub struct DynObjectStore(pub Box<dyn ObjectStore>);

// Implement Deref to allow access to the inner `ObjectStore` methods
impl Deref for DynObjectStore {
    type Target = dyn ObjectStore;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DynObjectStore {
    fn new<T: ObjectStore + 'static>(store: T) -> Self {
        DynObjectStore(Box::new(store) as Box<dyn ObjectStore>)
    }
}

/// The error type for [`CloudFile`](struct.CloudFile.html) methods.
#[derive(Error, Debug)]
pub enum CloudFileError {
    /// An error from [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    /// An path-related error from [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store) crate
    #[error("Object store path error: {0}")]
    ObjectStorePathError(#[from] object_store::path::Error),

    /// An error related to converting bytes into UTF-8
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    /// An error related to parsing a URL string
    #[error("Cannot parse URL: {0} {1}")]
    CannotParseUrl(String, String),

    /// An error related to creating a URL from a file path
    #[error("Cannot create URL from this absolute file path: '{0}'")]
    CannotCreateUrlFromFilePath(String),
}

#[tokio::test]
async fn cloud_file_2() -> Result<(), CloudFileError> {
    let cloud_file = CloudFile::new(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed",
        
    )?;
    assert_eq!(cloud_file.size().await?, 303);
    Ok(())
}

#[tokio::test]
async fn line_n() -> Result<(), CloudFileError> {
    use std::str::from_utf8;
    use futures_util::StreamExt;

    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
    let goal_index = 12;

    let cloud_file = CloudFile::new(url)?;
    let mut line_chunks = cloud_file.line_chunks().await?;
    let mut index_iter = 0..;
    let mut goal_line = None;
    'outer_loop: while let Some(line_chunk) = line_chunks.next().await {
        let line_chunk = line_chunk?;
        let lines = from_utf8(&line_chunk)?.split_terminator('\n');
        for line in lines {
            let index = index_iter.next().unwrap(); // safe because we know the iterator is infinite
            if index == goal_index {
                goal_line = Some(line.to_string());
                break 'outer_loop;
            }
        }
    }

    assert_eq!(goal_line, Some("per12 per12 0 0 2 -0.0382707".to_string()));
    Ok(())
}


#[tokio::test]
async fn cloud_file_extension() -> Result<(), CloudFileError> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    let mut cloud_file = CloudFile::new(url)?;
    assert_eq!(cloud_file.size().await?, 303);
    cloud_file.set_extension("fam")?;
    assert_eq!(cloud_file.size().await?, 130);
    Ok(())
}

// The AWS tests are skipped to credentials are not available.
#[tokio::test]
async fn s3_play_cloud() -> Result<(), CloudFileError> {
    use rusoto_credential::{CredentialsError, ProfileProvider, ProvideAwsCredentials};
    let credentials = if let Ok(provider) = ProfileProvider::new() {
        provider.credentials().await
    } else {
        Err(CredentialsError::new("No credentials found"))
    };

    let Ok(credentials) = credentials else {
        eprintln!("Skipping test because no AWS credentials found");
        return Ok(());
    };

    let url = "s3://bedreader/v1/toydata.5chrom.bed";
    let options = [
        ("aws_region", "us-west-2"),
        ("aws_access_key_id", credentials.aws_access_key_id()),
        ("aws_secret_access_key", credentials.aws_secret_access_key()),
    ];

    let cloud_file = CloudFile::new_with_options(url, options)?;
    assert_eq!(cloud_file.size().await?, 1_250_003);
    Ok(())
}

/// Given a local file's absolute path, return a URL string to that file.
/// 
/// # Example
/// ```
/// use cloud_file::abs_path_to_url_string;
/// 
/// let file_name = r"M:\data files\small.bed";
/// let url = abs_path_to_url_string(file_name)?;
/// assert_eq!(url, "file:///M:/data%20files/small.bed");
 /// # use cloud_file::CloudFileError;
 /// # Ok::<(), CloudFileError>(())
 /// ```
pub fn abs_path_to_url_string(path: impl AsRef<Path>) -> Result<String, CloudFileError> {
    let path = path.as_ref();
    let url = Url::from_file_path(path)
        .map_err(|_e| {
            CloudFileError::CannotCreateUrlFromFilePath(path.to_string_lossy().to_string())
        })?
        .to_string();
    Ok(url)
}

#[test]
fn readme_1() {
    use futures_util::StreamExt;
    use tokio::runtime::Runtime;

    Runtime::new()
        .unwrap()
        .block_on(async {
            let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/toydata.5chrom.fam";
            let cloud_file = CloudFile::new(url)?;
            let mut stream = cloud_file.open().await?;
            let mut newline_count: usize = 0;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                newline_count += bytecount::count(&bytes, b'\n');
            }
            assert_eq!(newline_count, 500);
            Ok::<(), CloudFileError>(())
        })
        .unwrap();
}

// #[test]
// fn temp_cmk() {
//     let file_name = r"M:\data files\small.bed";
//     let url = abs_path_to_url_string(file_name).unwrap();
//     println!("{url}");
// }

// cmk be sure to turn on discussion
