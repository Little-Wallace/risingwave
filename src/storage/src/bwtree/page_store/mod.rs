mod segment_file;

use std::sync::Arc;

use await_tree::InstrumentAwait;
use risingwave_common::cache::{CacheableEntry, LruCache};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    BlockLocation, InMemObjectStore, MonitoredStreamingUploader, ObjectStore, ObjectStoreImpl,
    ObjectStoreRef,
};
use risingwave_pb::bwtree::PageStoreVersion;
use risingwave_pb::hummock::SstableInfo;

use crate::bwtree::base_page::BasePage;
use crate::bwtree::leaf_page::{Delta, LeafPage};
use crate::bwtree::page_store::segment_file::{SegmentBuilder, SegmentMeta};
use crate::bwtree::smo::CheckpointData;
use crate::bwtree::PageId;
use crate::hummock::{CompressionAlgorithm, HummockError, HummockResult};

pub type PageStoreRef = Arc<PageStore>;
pub type SegmentHolder = CacheableEntry<HummockSstableObjectId, Box<SegmentMeta>>;

pub struct PageStore {
    object_store: ObjectStoreRef,
    path: String,
    meta_cache: Arc<LruCache<HummockSstableObjectId, Box<SegmentMeta>>>,
    page_store_version: PageStoreVersion,
}

impl PageStore {
    pub fn for_test() -> Arc<Self> {
        Arc::new(Self {
            object_store: Arc::new(ObjectStoreImpl::InMem(
                InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
            )),
            path: "".to_string(),
            meta_cache: Arc::new(LruCache::new(0, 4096)),
            page_store_version: PageStoreVersion {
                id: 0,
                tables: Default::default(),
                max_committed_epoch: 0,
                safe_epoch: 0,
                table_infos: vec![],
                total_file_size: 0,
            },
        })
    }

    pub async fn get_data_page(&self, pid: PageId) -> HummockResult<LeafPage> {
        let mut deltas = vec![];
        for segment_info in self.page_store_version.table_infos.iter().rev() {
            let segment = self.get_segment_meta(segment_info).await?;
            let delta_pos = segment.value().get_delta_offset(pid);
            if !delta_pos.is_empty() {
                let segment_path = self.get_segment_data_path(segment_info.object_id);
                // TODO: the delta of the same page would be continuous.
                for (pos, len) in delta_pos {
                    let loc = BlockLocation {
                        offset: pos,
                        size: len,
                    };
                    let buf = self
                        .object_store
                        .read(&segment_path, Some(loc))
                        .await
                        .map_err(HummockError::object_io_error)?;
                    deltas.push(Arc::new(Delta::decode(buf)));
                }
            }
            if let Some((pos, len)) = segment.value().get_page_offset(pid) {
                let segment_path = self.get_segment_data_path(segment_info.object_id);
                let loc = BlockLocation {
                    offset: pos,
                    size: len,
                };
                let buf = self
                    .object_store
                    .read(&segment_path, Some(loc))
                    .await
                    .map_err(HummockError::object_io_error)?;
                let page = Arc::new(BasePage::decode(buf)?);
                let mut leaf_page = LeafPage::new(page);
                for delta in deltas {
                    leaf_page.ingest(delta);
                }
                return Ok(leaf_page);
            }
        }
        Err(HummockError::other(format!("failed to read page {}", pid)))
    }

    pub async fn open_uploader(&self, path: &str) -> HummockResult<MonitoredStreamingUploader> {
        let uploader = self.object_store.streaming_upload(path)?;
        Ok(uploader)
    }

    pub fn get_segment_data_path(&self, object_id: HummockSstableObjectId) -> String {
        let obj_prefix = self.object_store.get_object_prefix(object_id, true);
        format!("{}/{}{}.data", self.path, obj_prefix, object_id)
    }

    pub async fn get_segment_meta(
        &self,
        object_info: &SstableInfo,
    ) -> HummockResult<SegmentHolder> {
        let entry = self
            .meta_cache
            .lookup_with_request_dedup::<_, HummockError, _>(
                object_info.object_id,
                object_info.object_id,
                || {
                    let store = self.object_store.clone();
                    let segment_path = self.get_segment_data_path(object_info.object_id);
                    let loc = BlockLocation {
                        offset: object_info.meta_offset as usize,
                        size: (object_info.file_size - object_info.meta_offset) as usize,
                    };
                    async move {
                        let buf = store
                            .read(&segment_path, Some(loc))
                            .await
                            .map_err(HummockError::object_io_error)?;
                        let charge = buf.len();
                        let meta = SegmentMeta::decode_from(&mut &buf[..])?;
                        Ok((Box::new(meta), charge))
                    }
                },
            )
            .verbose_instrument_await("meta_cache_lookup")
            .await?;
        Ok(entry)
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
