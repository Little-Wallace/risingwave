use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use risingwave_hummock_sdk::HummockSstableObjectId;

use crate::bwtree::PageId;
use crate::hummock::HummockResult;

#[async_trait]
pub trait PageIdGenerator: Send + Sync {
    async fn get_new_page_id(&self) -> HummockResult<PageId>;
    async fn get_new_page_ids(&self, count: usize) -> HummockResult<Vec<PageId>>;
    async fn get_segment_id(&self) -> HummockResult<HummockSstableObjectId>;
}

pub struct LocalPageIdGenerator {
    id: AtomicU64,
    sst_id: AtomicU64,
}

impl Default for LocalPageIdGenerator {
    fn default() -> Self {
        Self {
            id: AtomicU64::new(1),
            sst_id: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl PageIdGenerator for LocalPageIdGenerator {
    async fn get_new_page_id(&self) -> HummockResult<PageId> {
        let next_id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(next_id)
    }

    async fn get_segment_id(&self) -> HummockResult<HummockSstableObjectId> {
        let next_id = self
            .sst_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(next_id)
    }

    async fn get_new_page_ids(&self, count: usize) -> HummockResult<Vec<PageId>> {
        assert!(count > 0);
        let count = count as u64;
        let next_id = self
            .id
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        let mut ret = vec![];
        for idx in 0..count {
            ret.push(next_id + idx);
        }
        Ok(ret)
    }
}
