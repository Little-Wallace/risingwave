use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::catalog::TableId;

use crate::bwtree::root_page::RootPage;
use crate::hummock::HummockResult;

pub struct LocalStore {
    table_id: TableId,
    page: Arc<RootPage>,
}

pub struct BwTreeEngineCore {
    states: BTreeMap<TableId, Arc<RootPage>>,
}

impl BwTreeEngineCore {
    pub async fn flush_dirty_pages_before(&self, epoch: u64) -> HummockResult<()> {
        let mut tasks = vec![];
        for (_, page) in &self.states {
            let root = page.clone();
            let handle = tokio::spawn(async move {
                /// TODO: we must calculate min snapshot as the safe epoch to delete history version
                /// safely.
                root.flush_dirty_pages_before(epoch, epoch).await
            });
            tasks.push(handle);
        }
        let ret = try_join_all(tasks).await;
        Ok(())
    }
}
