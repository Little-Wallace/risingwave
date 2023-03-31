use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::TableKey;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::hummock::HummockResult;
use crate::storage_value::StorageValue;
use crate::store::WriteOptions;

pub struct LocalStore {
    table_id: TableId,
    page: Arc<BwTreeEngine>,
}

pub struct BwTreeEngineCore {
    states: BTreeMap<TableId, Arc<BwTreeEngine>>,
}

impl BwTreeEngineCore {
    /// This method only allow one thread calling.
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
        // TODO: retry or panic
        let ret = try_join_all(tasks).await;
        Ok(())
    }

    pub fn register_local_engine(&mut self, table_id: TableId, page: Arc<BwTreeEngine>) {
        self.states.insert(table_id, page);
    }
}

impl LocalStore {
    pub async fn get(&self, key: &Bytes, epoch: u64) -> HummockResult<Option<Bytes>> {
        self.page.get(TableKey(key.clone()), epoch).await
    }

    pub async fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> HummockResult<()> {
        self.page.flush(kv_pairs, write_options).await
    }
}
