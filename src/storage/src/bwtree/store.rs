use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo};
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::bwtree::bw_tree_engine::BwTreeEngine;
use crate::bwtree::bwtree_iterator::BwTreeIterator;
use crate::bwtree::index_page::PageType;
use crate::bwtree::page_id_generator::PageIdGenerator;
use crate::bwtree::page_store::PageStoreRef;
use crate::bwtree::PageId;
use crate::error::{StorageError, StorageResult};
use crate::hummock::{CompressionAlgorithm, HummockError, HummockResult};
use crate::mem_table::{merge_stream, KeyOp, MemTable};
use crate::storage_value::StorageValue;
use crate::store::{
    EmptyFutureTrait, GetFutureTrait, IterFutureTrait, IterKeyRange, LocalStateStore,
    MayExistTrait, NewLocalOptions, ReadOptions, StateStoreIter, StateStoreIterExt,
    StateStoreIterItem, StateStoreIterItemStream, StateStoreIterNextFutureTrait, StateStoreRead,
    StreamTypeOfIter, SyncFutureTrait, SyncResult, WriteOptions,
};
use crate::StateStore;

pub struct LocalBwTreeStore {
    table_id: TableId,
    engine: Arc<BwTreeEngine>,
    mem_table: MemTable,
    epoch: Option<u64>,
}

pub struct BwTreeEngineCore {
    states: BTreeMap<TableId, Arc<BwTreeEngine>>,
    page_store: PageStoreRef,
    page_id_manager: Arc<dyn PageIdGenerator>,
}

impl BwTreeEngineCore {
    /// This method only allow one thread calling.
    pub async fn flush_dirty_pages_before(&self, epoch: u64) -> HummockResult<SyncResult> {
        let mut tasks = vec![];
        for (table_id, state_table) in &self.states {
            let table_id = *table_id;
            let root = state_table.clone();
            let store = self.page_store.clone();
            let segment_id = self.page_id_manager.get_segment_id().await?;
            // TODO: we could calculate the size of shared-buffer of each table and then group them
            // to reduce write IO count.
            let handle: JoinHandle<
                HummockResult<(TableId, SstableInfo, HashMap<usize, (PageId, PageType)>)>,
            > = tokio::spawn(async move {
                // TODO: we must calculate min snapshot as the safe epoch to delete history version
                // safely.
                let checkpoint = root.flush_shared_buffer(epoch, epoch).await?;
                let mut uploader = store
                    .open_builder(segment_id, CompressionAlgorithm::None)
                    .await?;
                // TODO: we could flush some data to uploader during flush_shared_buffer, because we
                // do not need wait all pages.
                for page in checkpoint.leaf {
                    uploader.append_page(page).await?;
                }
                for (pid, delta) in checkpoint.leaf_deltas {
                    uploader.append_delta(pid, delta).await?;
                }
                for (pid, data) in checkpoint.index {
                    uploader.append_index_page(pid, epoch, data).await?;
                }
                uploader.append_redo_log(checkpoint.index_redo_log).await?;
                let sst_info = uploader.finish().await?;
                Ok((table_id, sst_info, checkpoint.vnodes))
            });
            tasks.push(handle);
        }
        // TODO: retry or panic
        let rets = try_join_all(tasks)
            .await
            .map_err(|_| HummockError::other("failed to finish sync task"))?;
        let mut uncommitted_ssts = vec![];
        let mut uncommitted_vnode_maps = vec![];
        for ret in rets {
            let (table_id, sst_info, vnodes_map) = ret?;
            uncommitted_ssts.push(LocalSstableInfo::new(
                StaticCompactionGroupId::StateDefault.into(),
                sst_info,
                TableStatsMap::default(),
            ));
            if !vnodes_map.is_empty() {
                let mut vnode_info = vnodes_map
                    .into_iter()
                    .map(|(vnode_id, (pid, ptype))| {
                        let v = ptype as u8;
                        (vnode_id, pid, v)
                    })
                    .collect_vec();
                vnode_info.sort_by_key(|x| x.0);
                uncommitted_vnode_maps.push((table_id, vnode_info));
            }
        }

        Ok(SyncResult {
            sync_size: 0,
            // uncommitted_vnode_maps
            uncommitted_ssts,
        })
    }
}
pub struct BwTreeStoreIterator {
    inner: BwTreeIterator,
}
#[try_stream(ok = StateStoreIterItem, error = StorageError)]
async fn into_stream(mut iter: BwTreeStoreIterator, table_id: TableId) {
    while let Some(chunk) = iter.next_chunk().await? {
        for (k, v) in chunk {
            yield (FullKey::new(table_id, TableKey(k), 0), v);
        }
    }
}
impl BwTreeStoreIterator {
    pub fn new(inner: BwTreeIterator) -> Self {
        Self { inner }
    }

    pub async fn next_chunk(&mut self) -> StorageResult<Option<Vec<(Bytes, Bytes)>>> {
        if self.inner.is_valid() {
            let chunk = self.inner.next_chunk().await?;
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }
}

pub struct SeverlBwTreeIterator {}

impl StateStoreIter for SeverlBwTreeIterator {
    type Item = StateStoreIterItem;

    type NextFuture<'a> = impl StateStoreIterNextFutureTrait<'a>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async { Ok(None) }
    }
}

impl LocalBwTreeStore {
    pub async fn get_inner(&self, key: Bytes, epoch: u64) -> StorageResult<Option<Bytes>> {
        let ret = self.engine.get(TableKey(key), epoch).await?;
        Ok(ret)
    }

    pub async fn iter_inner(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<impl StateStoreIterItemStream> {
        let table_id = read_options.table_id;
        let inner = self.engine.iter(key_range, epoch, read_options).await?;
        let iter = BwTreeStoreIterator::new(inner);
        Ok(into_stream(iter, table_id))
    }

    pub async fn may_exist_inner(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<bool> {
        return Ok(true);
    }
}

impl LocalStateStore for LocalBwTreeStore {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    define_local_state_store_associated_type!();

    fn get(&self, key: Bytes, _read_options: ReadOptions) -> Self::GetFuture<'_> {
        self.get_inner(key, self.epoch())
    }

    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
        async move {
            let table_id = read_options.table_id;
            assert_eq!(table_id, self.table_id);
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
                    KeyOp::Delete(_old_value) => {
                        kv_pairs.push((key, StorageValue::new_delete()));
                    }
                    KeyOp::Update((_old_value, new_value)) => {
                        kv_pairs.push((key, StorageValue::new_put(new_value)));
                    }
                }
            }
            Ok(self.engine.ingest_batch(
                kv_pairs,
                WriteOptions {
                    epoch: self.epoch(),
                    table_id: self.table_id,
                },
            ))
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        todo!()
    }

    fn init(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }

    fn seal_current_epoch(&mut self, _next_epoch: u64) {
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

#[derive(Clone)]
pub struct BwTreeStorage {
    core: Arc<tokio::sync::RwLock<BwTreeEngineCore>>,
}

impl BwTreeStorage {
    async fn sync_inner(&self, epoch: u64) -> StorageResult<SyncResult> {
        let ret = self
            .core
            .read()
            .await
            .flush_dirty_pages_before(epoch)
            .await?;
        Ok(ret)
    }

    async fn iter_inner(
        &self,
        _key_range: IterKeyRange,
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> StorageResult<StreamTypeOfIter<SeverlBwTreeIterator>> {
        unimplemented!("todo support iter inner")
    }

    async fn get_inner(
        &self,
        key: Bytes,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<Option<Bytes>> {
        let engine = {
            let guard = self.core.read().await;
            guard.states.get(&table_id).cloned()
        };
        let engine = match engine {
            Some(engine) => engine,
            None => return Ok(None),
        };
        let ret = engine.get(TableKey(key), epoch).await?;
        Ok(ret)
    }

    async fn new_local_inner(&self, _options: NewLocalOptions) -> LocalBwTreeStore {
        unimplemented!("");
    }
}

impl StateStoreRead for BwTreeStorage {
    type IterStream = StreamTypeOfIter<SeverlBwTreeIterator>;

    define_state_store_read_associated_type!();

    fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        self.get_inner(key, epoch, read_options.table_id)
    }

    fn iter(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        self.iter_inner(key_range, epoch, read_options)
    }
}

impl StateStore for BwTreeStorage {
    type Local = LocalBwTreeStore;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, _wait_epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move { Ok(()) }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        self.sync_inner(epoch)
    }

    fn seal_epoch(&self, epoch: u64, _is_checkpoint: bool) {
        if epoch == INVALID_EPOCH {
            warn!("sealing invalid epoch");
            return;
        }
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        // TODO: we must clean all dirty page and uncommited page.
        async { Ok(()) }
    }

    fn new_local(&self, option: NewLocalOptions) -> Self::NewLocalFuture<'_> {
        self.new_local_inner(option)
    }

    fn validate_read_epoch(&self, _epoch: HummockReadEpoch) -> StorageResult<()> {
        Ok(())
    }
}
