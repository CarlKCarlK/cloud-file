use anyhow::anyhow;
use bytes::Bytes;
use core::fmt;
use object_store::http::HttpBuilder;
use object_store::path::Path as StorePath;
use object_store::{GetOptions, GetResult, ObjectStore};
use std::ops::Deref;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
/// The location of a file in the cloud.
///
/// The location is made up of of two parts, an `Arc`-wrapped [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html)
/// and an [`object_store::path::Path as StorePath`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html).
/// The [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) is a cloud service, for example, Http, AWS S3, Azure,
/// the local file system, etc. The `StorePath` is the path to the file on the cloud service.
///
/// See ["Cloud URLs and `ObjectPath` Examples"](supplemental_document_cloud_urls/index.html) for details specifying a file.
///
/// An `ObjectPath` can be efficiently cloned because the `ObjectStore` is `Arc`-wrapped.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use object_store::{local::LocalFileSystem, path::Path as StorePath};
/// use bed_reader::{ObjectPath, BedErrorPlus, sample_bed_file};
///
/// # Runtime::new().unwrap().block_on(async {
/// let arc_object_store = Arc::new(LocalFileSystem::new()); // Arc-wrapped ObjectStore
/// let file_path = sample_bed_file("plink_sim_10s_100v_10pmiss.bed")?; // regular Rust PathBuf
/// let store_path = StorePath::from_filesystem_path(&file_path)?; // StorePath
/// let object_path = ObjectPath::new(arc_object_store, store_path); // ObjectPath
/// assert_eq!(object_path.size().await?, 303);
/// # Ok::<(), anyhow::Error>(())}).unwrap();
/// # use {tokio::runtime::Runtime};
/// ```
pub struct ObjectPath {
    /// An `Arc`-wrapped [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) cloud service, for example, Http, AWS S3,
    /// Azure, the local file system, etc.
    pub object_store: Arc<DynObjectStore>,
    /// A [`object_store::path::Path as StorePath`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html) that points to a file on
    /// the [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html)
    /// that gives the path to the file on the cloud service.
    pub path: StorePath,
}

impl Clone for ObjectPath {
    fn clone(&self) -> Self {
        ObjectPath {
            object_store: self.object_store.clone(),
            path: self.path.clone(),
        }
    }
}

/// An empty set of [cloud options](supplemental_document_options/index.html#cloud-options)
///
/// See ["Cloud URLs and `ObjectPath` Examples"](supplemental_document_cloud_urls/index.html) for examples.
pub const EMPTY_OPTIONS: [(&str, String); 0] = [];

impl ObjectPath {
    /// Create a new [`ObjectPath`] from a URL string and [cloud options](supplemental_document_options/index.html#cloud-options).
    ///
    /// See ["Cloud URLs and `ObjectPath` Examples"](supplemental_document_cloud_urls/index.html) for details specifying a file.
    ///
    /// # Example
    /// ```
    /// use std::sync::Arc;
    /// use object_store::{local::LocalFileSystem, path::Path as StorePath};
    /// use bed_reader::{ObjectPath, BedErrorPlus, sample_bed_url, EMPTY_OPTIONS};
    /// # Runtime::new().unwrap().block_on(async {
    /// let url: String = sample_bed_url("plink_sim_10s_100v_10pmiss.bed")?;
    /// let object_path: ObjectPath<_> = ObjectPath::from_url(url, EMPTY_OPTIONS)?;
    /// assert_eq!(object_path.size().await?, 303);
    /// # Ok::<(), anyhow::Error>(())}).unwrap();
    /// # use {tokio::runtime::Runtime};
    /// ```
    pub fn from_url<I, K, V, S>(
        // cmk should we call this 'new'?
        location: S,
        options: I,
    ) -> Result<ObjectPath, anyhow::Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
        S: AsRef<str>,
    {
        let location = location.as_ref();
        let url =
            Url::parse(location).map_err(|e| anyhow!("Cannot parse url: {} {}", location, e))?;

        let (object_store, store_path): (DynObjectStore, StorePath) =
            parse_url_opts_work_around(&url, options)?;
        let object_path = ObjectPath::new(Arc::new(object_store), store_path);
        Ok(object_path)
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
pub fn parse_url_opts_work_around<I, K, V>(
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
        let store = DynObjectStore::new(Box::new(builder.build()?)) as _;
        Ok((store, path))
    } else {
        let (store, path) = object_store::parse_url_opts(url, options)?;
        let store = DynObjectStore::new(store);
        Ok((store, path))
    }
}

impl ObjectPath {
    /// Create a new [`ObjectPath`] from an `Arc`-wrapped [`ObjectStore`](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html) and an [`object_store::path::Path as StorePath`](https://docs.rs/object_store/latest/object_store/path/struct.Path.html).
    ///
    /// Both parts must be owned, but see [`ObjectPath`] for examples of creating from a tuple with references.
    ///
    /// # Example
    /// ```
    /// use std::sync::Arc;
    /// use object_store::{local::LocalFileSystem, path::Path as StorePath};
    /// use bed_reader::{ObjectPath, BedErrorPlus, sample_bed_file};
    /// # Runtime::new().unwrap().block_on(async {
    /// let object_store = Arc::new(LocalFileSystem::new()); // Arc-wrapped ObjectStore
    /// let file_path = sample_bed_file("plink_sim_10s_100v_10pmiss.bed")?; // regular Rust PathBuf
    /// let store_path = StorePath::from_filesystem_path(&file_path)?; // StorePath
    ///
    /// let object_path: ObjectPath<_> = ObjectPath::new(object_store, store_path); // ObjectPath from owned values
    /// assert_eq!(object_path.size().await?, 303);
    /// # Ok::<(), anyhow::Error>(())}).unwrap();
    /// # use {tokio::runtime::Runtime};
    /// ```
    pub fn new(arc_object_store: Arc<DynObjectStore>, path: StorePath) -> Self {
        ObjectPath {
            object_store: arc_object_store,
            path,
        }
    }

    /// Return the size of a file stored in the cloud.
    ///
    /// # Example
    /// ```
    /// use bed_reader::{sample_bed_object_path};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let mut object_path = sample_bed_object_path("plink_sim_10s_100v_10pmiss.bed")?;
    /// assert_eq!(object_path.size().await?, 303);
    /// # Ok::<(), anyhow::Error>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, bed_reader::BedErrorPlus};
    /// ```
    pub async fn size(&self) -> Result<usize, anyhow::Error> {
        // cmk is this still needed?
        let get_result = self.get().await?;
        // LATER: See if https://github.com/apache/arrow-rs/issues/5272 if fixed in
        // a way so that only one read is needed.
        let object_meta = &get_result.meta;
        Ok(object_meta.size)
    }

    /// Return the bytes that are stored at the specified location in the given byte ranges
    pub async fn get_ranges(
        &self,
        ranges: &[core::ops::Range<usize>],
    ) -> Result<Vec<Bytes>, anyhow::Error> {
        Ok(self.object_store.get_ranges(&self.path, ranges).await?)
    }

    /// Perform a get request with options
    pub async fn get_opts(&self, get_options: GetOptions) -> Result<GetResult, anyhow::Error> {
        Ok(self.object_store.get_opts(&self.path, get_options).await?)
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(&self) -> Result<GetResult, anyhow::Error> {
        Ok(self.object_store.get(&self.path).await?)
    }

    /// Updates the [`ObjectPath`] to have the given extension.
    ///
    /// It removes the current extension, if any.
    /// It appends the given extension, if any.
    ///
    /// # Example
    /// ```
    /// use bed_reader::{sample_bed_object_path};
    ///
    /// # Runtime::new().unwrap().block_on(async {
    /// let mut object_path = sample_bed_object_path("plink_sim_10s_100v_10pmiss.bed")?;
    /// assert_eq!(object_path.size().await?, 303);
    /// object_path.set_extension("fam")?;
    /// assert_eq!(object_path.size().await?, 130);
    /// # Ok::<(), anyhow::Error>(())}).unwrap();
    /// # use {tokio::runtime::Runtime, bed_reader::BedErrorPlus};
    /// ```
    pub fn set_extension(&mut self, extension: &str) -> Result<(), anyhow::Error> {
        let mut path_str = self.path.to_string();

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
        self.path = StorePath::parse(&path_str)?;
        Ok(())
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectPath: {:?}", self.path)
    }
}

/// Newtype wrapping the `Box<dyn ObjectStore>` for easier usage
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
    pub fn new(store: Box<dyn ObjectStore>) -> Self {
        DynObjectStore(store)
    }
}
