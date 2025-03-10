use std::fmt::Debug;
use std::ops::Range;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};
use pprof::Profiler;

pub struct ProfilingObjectStore {
    profiler: Profiler,
}

impl ProfilingObjectStore {
    pub fn new() -> Self {
        Self {
            profiler: Profiler::new().unwrap(),
        }
    }
}

impl Default for ProfilingObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ProfilingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        todo!()
        // self.put_opts(location, payload, PutOptions::default())
        //     .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        todo!();
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        // self.put_multipart_opts(location, PutMultipartOpts::default())
        //     .await
        todo!();
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        todo!();
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        // self.get_opts(location, GetOptions::default()).await
        todo!();
    }


    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        todo!();
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        // let options = GetOptions {
        //     range: Some(range.into()),
        //     ..Default::default()
        // };
        // self.get_opts(location, options).await?.bytes().await
        todo!()
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        // coalesce_ranges(
        //     ranges,
        //     |range| self.get_range(location, range),
        //     OBJECT_STORE_COALESCE_DEFAULT,
        // )
        // .await
        todo!()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        // let options = GetOptions {
        //     head: true,
        //     ..Default::default()
        // };
        // Ok(self.get_opts(location, options).await?.meta)
        todo!()
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()> {
        todo!();
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        // locations
        //     .map(|location| async {
        //         let location = location?;
        //         self.delete(&location).await?;
        //         Ok(location)
        //     })
        //     .buffered(10)
        //     .boxed()
        todo!();
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        todo!();
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        // let offset = offset.clone();
        // self.list(prefix)
        //     .try_filter(move |f| futures::future::ready(f.location > offset))
        //     .boxed()
        todo!()
    }


    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        todo!();
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        todo!()
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // self.copy(from, to).await?;
        // self.delete(from).await
        todo!();
    }


    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        todo!();
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        // self.copy_if_not_exists(from, to).await?;
        // self.delete(from).await
        todo!()
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
