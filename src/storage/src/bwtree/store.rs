use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::TableKey;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::error::StorageResult;
use crate::hummock::HummockResult;
use crate::mem_table::{merge_stream, KeyOp, MemTable};
use crate::storage_value::StorageValue;
use crate::store::{
    GetFutureTrait, IterKeyRange, LocalStateStore, MayExistTrait, ReadOptions, StateStoreIter,
    StateStoreIterExt, StateStoreIterItem, StateStoreIterItemStream, StateStoreIterNextFutureTrait,
    StreamTypeOfIter, WriteOptions,
};

pub struct LocalStore {
    table_id: TableId,
    page: Arc<BwTreeEngine>,
    mem_table: MemTable,
    epoch: Option<u64>,
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
                // TODO: we must calculate min snapshot as the safe epoch to delete history version
                // safely.
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

pub struct BwTreeIterator {}

impl StateStoreIter for BwTreeIterator {
    type Item = StateStoreIterItem;

    type NextFuture<'a> = impl StateStoreIterNextFutureTrait<'a>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async { Ok(None) }
    }
}

impl LocalStore {
    pub async fn get_inner(&self, key: Bytes, epoch: u64) -> StorageResult<Option<Bytes>> {
        let ret = self.page.get(TableKey(key), epoch).await?;
        Ok(ret)
    }

    pub async fn ingest_batch_inner(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        let sz = self.page.flush(kv_pairs, write_options).await?;
        Ok(sz)
    }

    pub async fn iter_inner(
        &self,
        _key_range: IterKeyRange,
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<BwTreeIterator>> {
        let iter = BwTreeIterator {};
        Ok(iter.into_stream())
    }

    pub async fn may_exist_inner(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<bool> {
        return Ok(true);
    }
}

impl LocalStateStore for LocalStore {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    define_local_state_store_associated_type!();

    fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
        self.get_inner(key, self.epoch())
    }

    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
        async move {
            let stream = self
                .iter_inner(key_range.clone(), self.epoch(), read_options)
                .await?;
            let (l, r) = key_range;
            let key_range = (l.map(Bytes::from), r.map(Bytes::from));
            Ok(merge_stream(
                self.mem_table.iter(key_range),
                stream,
                self.table_id,
                self.epoch(),
            ))
        }
    }

    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };
        Ok(())
    }

    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    fn flush(&mut self, _delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
        async move {
            // TODO: filter it with delete-ranges after bw-tree support range delete.
            let buffer = self.mem_table.drain().into_parts();
            let mut kv_pairs = Vec::with_capacity(buffer.len());
            for (key, key_op) in buffer.into_iter() {
                match key_op {
                    KeyOp::Insert(value) => {
                        kv_pairs.push((key, StorageValue::new_put(value)));
                    }
                    KeyOp::Delete(old_value) => {
                        kv_pairs.push((key, StorageValue::new_delete()));
                    }
                    KeyOp::Update((old_value, new_value)) => {
                        kv_pairs.push((key, StorageValue::new_put(new_value)));
                    }
                }
            }
            self.ingest_batch_inner(
                kv_pairs,
                WriteOptions {
                    epoch: self.epoch(),
                    table_id: self.table_id,
                },
            )
            .await
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        todo!()
    }

    fn init(&mut self, epoch: u64) {
        todo!()
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) {
        todo!()
    }

    fn may_exist(
        &self,
        key_range: IterKeyRange,
        read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        self.may_exist_inner(key_range, read_options)
    }
}
