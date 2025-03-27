use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    coalesce_ranges, OBJECT_STORE_COALESCE_DEFAULT,
    DynObjectStore, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result
};
use parking_lot::RwLock;
use pprof::{Profiler, sample_current, Result as PprofResult};

pub struct ProfilingObjectStore {
    // TODO need a better way than all this being pub to have a shared reference to the profilers ... 

    pub inner: Arc<DynObjectStore>,
    // TODO having rwlocks for these is probably not best
    // we could probably instead have a background thread
    // reading samples from a channel that updates these
    pub get_profiler: Arc<RwLock<PprofResult<Profiler>>>,

    pub put_profiler: Arc<RwLock<PprofResult<Profiler>>>,
}

// impl ProfilingObjectStore {
//     pub fn new(inner: Arc<DynObjectStore>) -> Self {
//         Self {
//             inner,
//             get_profiler: Arc::new(RwLock::new(Profiler::new().unwrap())),
//             put_profiler: Arc::new(RwLock::new(Profiler::new().unwrap())),
//         }
//     }
// }


#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ProfilingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.put_opts(location, payload, PutOptions::default())
            .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        {
            let mut guard = self.put_profiler.write();
            println!("doing put");
            guard.as_mut().unwrap().sample(sample_current(1));
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOpts::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }


    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        {
            let mut guard = self.get_profiler.write();
            println!("doing get");
            guard.as_mut().unwrap().sample(sample_current(1));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let options = GetOptions {
            range: Some(range.into()),
            ..Default::default()
        };
        self.get_opts(location, options).await?.bytes().await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        coalesce_ranges(
            ranges,
            |range| self.get_range(location, range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        Ok(self.get_opts(location, options).await?.meta)
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .map(|location| async {
                let location = location?;
                self.delete(&location).await?;
                Ok(location)
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |f| futures::future::ready(f.location > offset))
            .boxed()
    }


    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }


    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_if_not_exists(from, to).await?;
        self.delete(from).await
    }
}

impl Debug for ProfilingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfilingObjectStore").finish()
    }
}

impl std::fmt::Display for ProfilingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProfilingObjectStore")
    }
}
