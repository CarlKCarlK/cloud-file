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
//! ## Main Functions cmk
// cmk like bed-reader, but with links to examples and maybe supplemental doc

use bytes::Bytes;
use core::fmt;
use futures_util::TryStreamExt;
use object_store::http::HttpBuilder;
use object_store::path::Path as StorePath;
use object_store::{GetOptions, GetResult, ObjectStore};
use std::ops::{Deref, Range};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

#[derive(Debug)]
/// The main struct representing the location of a file in the cloud.
///
/// cmk change ObjectStore to DynObjectStore
/// The location is made up of of two parts, an `Arc`-wrapped [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html)
/// and an [`object_store::path::Path as StorePath`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html).
/// The [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) is a cloud service, for example, Http, AWS S3, Azure,
/// the local file system, etc. The `StorePath` is the path to the file on the cloud service.
///
/// See ["Cloud URLs and `CloudFile` Examples"](supplemental_document_cloud_urls/index.html) for details specifying a file.
///
/// An `CloudFile` can be efficiently cloned because the `ObjectStore` is `Arc`-wrapped.
/// /// cmk change ObjectStore to DynObjectStore
///
/// # Examples
///
/// ```
/// use cloud_file::{CloudFile, CloudFileError, EMPTY_OPTIONS};
///
/// # Runtime::new().unwrap().block_on(async {
/// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
/// let cloud_file = CloudFile::new(&url, EMPTY_OPTIONS)?;
/// assert_eq!(cloud_file.size().await?, 303);
/// # Ok::<(), CloudFileError>(())}).unwrap();
/// # use {tokio::runtime::Runtime};
/// ```
pub struct CloudFile {
    /// cmk change ObjectStore to DynObjectStore
    /// An `Arc`-wrapped [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) cloud service, for example, Http, AWS S3,
    /// Azure, the local file system, etc.
    pub arc_object_store: Arc<DynObjectStore>,
    /// A [`object_store::path::Path as StorePath`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html) that points to a file on
    /// the [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html)
    /// that gives the path to the file on the cloud service.
    pub store_path: StorePath,
}

impl Clone for CloudFile {
    fn clone(&self) -> Self {
        CloudFile {
            arc_object_store: self.arc_object_store.clone(),
            store_path: self.store_path.clone(),
        }
    }
}

// cmk update reference
/// An empty set of [cloud options](supplemental_document_options/index.html#cloud-options)
///
/// See ["Cloud URLs and `CloudFile` Examples"](supplemental_document_cloud_urls/index.html) for examples.
pub const EMPTY_OPTIONS: [(&str, String); 0] = [];

impl CloudFile {
    /// Create a new [`CloudFile`] from a URL string and [cloud options](supplemental_document_options/index.html#cloud-options).
    ///
    /// See ["Cloud URLs and `CloudFile` Examples"](supplemental_document_cloud_urls/index.html) for details specifying a file.
    ///
    /// # Example
    /// ```
    /// use cloud_file::{CloudFile, CloudFileError, EMPTY_OPTIONS};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(url, EMPTY_OPTIONS)?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime};
    /// ```
    pub fn new<I, K, V>(location: impl AsRef<str>, options: I) -> Result<CloudFile, CloudFileError>
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
            arc_object_store: Arc::new(object_store),
            store_path,
        };
        Ok(cloud_file)
    }

    /// cmk create docs
    pub async fn line_count(&self) -> Result<usize, CloudFileError> {
        let stream = self.get().await?.into_stream();

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
    /// use cloud_file::{CloudFile, EMPTY_OPTIONS};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let cloud_file = CloudFile::new(&url, EMPTY_OPTIONS)?;
    /// assert_eq!(cloud_file.size().await?, 303);
    /// # Ok::<(), CloudFileError>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, cloud_file::CloudFileError};
    /// ```
    pub async fn size(&self) -> Result<usize, CloudFileError> {
        let meta = self.arc_object_store.head(&self.store_path).await?;
        Ok(meta.size)
    }

    /// cmk need an example
    /// Return the bytes that are stored at the specified location(s) in the given byte ranges
    pub async fn get_ranges(&self, ranges: &[Range<usize>]) -> Result<Vec<Bytes>, CloudFileError> {
        Ok(self
            .arc_object_store
            .get_ranges(&self.store_path, ranges)
            .await?)
    }

    /// Perform a get request with options
    /// cmk need an example
    pub async fn get_opts(&self, get_options: GetOptions) -> Result<GetResult, CloudFileError> {
        Ok(self
            .arc_object_store
            .get_opts(&self.store_path, get_options)
            .await?)
    }

    /// Return the bytes that are stored at the specified location.
    /// cmk need an example
    pub async fn get(&self) -> Result<GetResult, CloudFileError> {
        Ok(self.arc_object_store.get(&self.store_path).await?)
    }

    /// Updates the [`CloudFile`] in place to have the given extension.
    ///
    /// It removes the current extension, if any.
    /// It appends the given extension, if any.
    ///
    /// The method is in-place rather than functional to make it consistent with
    /// [`std::path::PathBuf::set_extension`](https://doc.rust-lang.org/stable/std/path/struct.PathBuf.html#method.set_extension).
    ///
    /// # Example
    /// ```
    /// use cloud_file::{CloudFile, EMPTY_OPTIONS};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    /// let mut cloud_file = CloudFile::new(&url, EMPTY_OPTIONS)?;
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

/// Wraps `Box<dyn ObjectStore>` for easier usage
#[derive(Debug)]
pub struct DynObjectStore(Box<dyn ObjectStore>);

// Implement Deref to allow access to the inner `ObjectStore` methods
impl Deref for DynObjectStore {
    type Target = dyn ObjectStore;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DynObjectStore {
    pub fn new<T: ObjectStore + 'static>(store: T) -> Self {
        DynObjectStore(Box::new(store) as Box<dyn ObjectStore>)
    }
}

#[derive(Error, Debug)]
pub enum CloudFileError {
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("Object store path error: {0}")]
    ObjectStorePathError(#[from] object_store::path::Error),

    #[error("Cannot parse URL: {0} {1}")]
    CannotParseUrl(String, String),

    #[allow(missing_docs)]
    #[error("Cannot create URL from this absolute file path: '{0}'")]
    CannotCreateUrlFromFilePath(String),
}

#[tokio::test]
async fn cloud_file_2() -> Result<(), CloudFileError> {
    let cloud_file = CloudFile::new(
        "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed",
        EMPTY_OPTIONS,
    )?;
    assert_eq!(cloud_file.size().await?, 303);
    Ok(())
}

#[tokio::test]
async fn cloud_file_extension() -> Result<(), CloudFileError> {
    let url = "https://raw.githubusercontent.com/fastlmm/bed-sample-files/main/plink_sim_10s_100v_10pmiss.bed";
    let mut cloud_file = CloudFile::new(url, EMPTY_OPTIONS)?;
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

    let cloud_file = CloudFile::new(url, options)?;
    assert_eq!(cloud_file.size().await?, 1_250_003);
    Ok(())
}

/// Returns a local file's absolute path as a URL string, taking care of any needed encoding.
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
            let cloud_file = CloudFile::new(url,EMPTY_OPTIONS)?;
            let mut stream = cloud_file.get().await?.into_stream();
            let mut newline_count: usize = 0;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                let count = bytecount::count(&bytes, b'\n');
                newline_count += count;
            }
            assert_eq!(newline_count, 500);
            Ok::<(), CloudFileError>(())
        })
        .unwrap();
}

// cmk understand these
// // Fetch just the file metadata
// let meta = object_store.head(&path).await.unwrap();
// println!("{meta:?}");
//
// // Fetch the object including metadata
// let result: GetResult = object_store.get(&path).await.unwrap();
// assert_eq!(result.meta, meta);
//
// // Buffer the entire object in memory
// let object: Bytes = result.bytes().await.unwrap();
// assert_eq!(object.len(), meta.size);
//
// // Alternatively stream the bytes from object storage
// let stream = object_store.get(&path).await.unwrap().into_stream();
//
// // Count the '0's using `try_fold` from `TryStreamExt` trait
// let num_zeros = stream
//     .try_fold(0, |acc, bytes| async move {
//         Ok(acc + bytes.iter().filter(|b| **b == 0).count())
//     }).await.unwrap();
//

// cmk need README.md etc.
// cmk set up the warnings for missing docs, etc
// cmk add 64-bit test
// cmk how can docs reference examples?
// cmk make an example (and/or a method) for random region reading.
// cmk limitations: no writing, no directories, no in-memory support, a bit-less efficient than generics,
// cmk limitations: no option of which services, makes non-url usage awkward.
// cmk be sure to turn on discussion
