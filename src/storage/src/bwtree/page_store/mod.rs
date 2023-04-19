mod segment_file;

use std::sync::Arc;

use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};

use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::smo::CheckpointData;
use crate::bwtree::PageId;
use crate::hummock::HummockResult;

pub type PageStoreRef = Arc<PageStore>;

pub struct PageStore {
    object_store: ObjectStoreImpl,
}

impl PageStore {
    pub fn for_test() -> Self {
        Self {
            object_store: ObjectStoreImpl::InMem(
                InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
            ),
        }
    }

    pub async fn get_data_page(&self, _pid: PageId) -> HummockResult<LeafPage> {
        unimplemented!()
    }

    pub async fn sync(&self) -> HummockResult<()> {
        Ok(())
    }
}
