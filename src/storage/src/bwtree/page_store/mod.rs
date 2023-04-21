mod segment_file;

use std::sync::Arc;

use risingwave_common::cache::LruCache;
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredStreamingUploader, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
};

use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::page_store::segment_file::{SegmentBuilder, SegmentMeta};
use crate::bwtree::smo::CheckpointData;
use crate::bwtree::PageId;
use crate::hummock::{CompressionAlgorithm, HummockResult};

pub type PageStoreRef = Arc<PageStore>;

pub struct PageStore {
    object_store: ObjectStoreRef,
    path: String,
    meta_cache: Arc<LruCache<HummockSstableObjectId, Box<SegmentMeta>>>,
}

impl PageStore {
    pub fn for_test() -> Self {
        Self {
            object_store: Arc::new(ObjectStoreImpl::InMem(
                InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
            )),
            path: "".to_string(),
            meta_cache: Arc::new(LruCache::new(0, 4096)),
        }
    }

    pub async fn get_data_page(&self, _pid: PageId) -> HummockResult<LeafPage> {
        unimplemented!()
    }

    pub async fn open_uploader(&self, path: &str) -> HummockResult<MonitoredStreamingUploader> {
        let uploader = self.object_store.streaming_upload(path)?;
        Ok(uploader)
    }

    pub fn get_segment_data_path(&self, object_id: HummockSstableObjectId) -> String {
        let obj_prefix = self.object_store.get_object_prefix(object_id, true);
        format!("{}/{}{}.data", self.path, obj_prefix, object_id)
    }

    pub async fn open_builder(
        &self,
        object_id: HummockSstableObjectId,
        algothrim: CompressionAlgorithm,
    ) -> HummockResult<SegmentBuilder> {
        let path = self.get_segment_data_path(object_id);
        let uploader = self.open_uploader(&path).await?;
        Ok(SegmentBuilder::open(object_id, algothrim, uploader))
    }
}
