use std::sync::atomic::AtomicU64;

use async_trait::async_trait;

use crate::bwtree::PageId;
use crate::hummock::HummockResult;

#[async_trait]
pub trait PageIdGenerator: Send + Sync {
    async fn get_new_page_id(&self) -> HummockResult<PageId>;
}

pub struct LocalPageIdGenerator {
    id: AtomicU64,
}

impl Default for LocalPageIdGenerator {
    fn default() -> Self {
        Self {
            id: AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl PageIdGenerator for LocalPageIdGenerator {
    async fn get_new_page_id(&self) -> HummockResult<PageId> {
        let next_id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(next_id)
    }
}
