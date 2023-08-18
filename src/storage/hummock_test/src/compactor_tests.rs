// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::ops::Bound;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;

    use bytes::{BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use rand::prelude::ThreadRng;
    use rand::rngs::StdRng;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::cache::CachePriority;
    use risingwave_common::catalog::TableId;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::Epoch;
    use risingwave_common_service::observer_manager::NotificationClient;
    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
        GroupDeltasSummary, HummockLevelsExt, HummockVersionExt,
    };
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{next_key, FullKey, TableKey, TABLE_PREFIX_LEN};
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
    use risingwave_hummock_sdk::HummockCompactionTaskId;
    use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use risingwave_meta::hummock::compaction::{
        default_level_selector, CompactStatus, LevelSelector, LocalSelectorStatistic,
        ManualCompactionOption,
    };
    use risingwave_meta::hummock::model::CompactionGroup;
    use risingwave_meta::hummock::test_utils::{
        register_table_ids_to_compaction_group, setup_compute_env, setup_compute_env_with_config,
        unregister_table_ids_from_compaction_group,
    };
    use risingwave_meta::hummock::{HummockManagerRef, LevelHandler, MockHummockMetaClient};
    use risingwave_meta::storage::MetaStore;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::hummock::compact_task::TaskStatus;
    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::{
        CompactTask, HummockVersion, Level, LevelType, OverlappingLevel, SstableInfo, TableOption,
    };
    use risingwave_pb::meta::add_worker_node_request::Property;
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::filter_key_extractor::{
        FilterKeyExtractorImpl, FilterKeyExtractorManagerRef, FixedLengthFilterKeyExtractor,
        FullKeyFilterKeyExtractor,
    };
    use risingwave_storage::hummock::compactor::{
        CompactionExecutor, Compactor, CompactorContext, ConcatSstableIterator,
        DummyCompactionFilter, TaskConfig, TaskProgress,
    };
    use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
    use risingwave_storage::hummock::iterator::UnorderedMergeIteratorInner;
    use risingwave_storage::hummock::multi_builder::{
        CapacitySplitTableBuilder, LocalTableBuilderFactory,
    };
    use risingwave_storage::hummock::sstable_store::SstableStoreRef;
    use risingwave_storage::hummock::value::HummockValue;
    use risingwave_storage::hummock::{
        BlockedXor16FilterBuilder, CachePolicy, CompactionDeleteRanges,
        HummockStorage as GlobalHummockStorage, HummockStorage, MemoryLimiter, SstableBuilder,
        SstableBuilderOptions, SstableObjectIdManager, SstableWriterOptions,
    };
    use risingwave_storage::monitor::{CompactorMetrics, StoreLocalStatistic};
    use risingwave_storage::opts::StorageOpts;
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::*;

    use crate::get_notification_client_for_test;
    use crate::test_utils::{register_tables_with_id_for_test, TestIngestBatch};

    pub(crate) async fn get_hummock_storage<S: MetaStore>(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        hummock_manager_ref: &HummockManagerRef<S>,
        table_id: TableId,
    ) -> HummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();

        let hummock = GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap();

        register_tables_with_id_for_test(
            hummock.filter_key_extractor_manager(),
            hummock_manager_ref,
            &[table_id.table_id()],
        )
        .await;

        hummock
    }

    async fn get_global_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> GlobalHummockStorage {
        let remote_dir = "hummock_001_test".to_string();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store();

        GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap()
    }

    async fn prepare_test_put_data(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        key: &Bytes,
        value_size: usize,
        epochs: Vec<u64>,
    ) {
        let mut local = storage.new_local(Default::default()).await;
        // 1. add sstables
        let val = b"0"[..].repeat(value_size);
        local.init(epochs[0]);
        for (i, &epoch) in epochs.iter().enumerate() {
            let mut new_val = val.clone();
            new_val.extend_from_slice(&epoch.to_be_bytes());
            local
                .ingest_batch(
                    vec![(key.clone(), StorageValue::new_put(Bytes::from(new_val)))],
                    vec![],
                    WriteOptions {
                        epoch,
                        table_id: Default::default(),
                    },
                )
                .await
                .unwrap();
            if i + 1 < epochs.len() {
                local.seal_current_epoch(epochs[i + 1]);
            } else {
                local.seal_current_epoch(u64::MAX);
            }
            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }
    }

    fn get_compactor_context_with_filter_key_extractor_manager(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> CompactorContext {
        get_compactor_context_with_filter_key_extractor_manager_impl(
            storage.storage_opts().clone(),
            storage.sstable_store(),
            hummock_meta_client,
            filter_key_extractor_manager,
        )
    }

    fn get_compactor_context_with_filter_key_extractor_manager_impl(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> CompactorContext {
        CompactorContext {
            storage_opts: options.clone(),
            sstable_store,
            hummock_meta_client: hummock_meta_client.clone(),
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
            memory_limiter: MemoryLimiter::unlimit(),
            filter_key_extractor_manager,
            sstable_object_id_manager: Arc::new(SstableObjectIdManager::new(
                hummock_meta_client.clone(),
                options.sstable_id_remote_fetch_number,
            )),
            task_progress_manager: Default::default(),
            await_tree_reg: None,
            running_task_count: Arc::new(AtomicU32::new(0)),
        }
    }

    #[tokio::test]
    async fn test_compaction_watermark() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .level0_max_compact_file_number(130)
            .level0_overlapping_sub_level_compact_level_count(1)
            .build();
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env_with_config(8080, config).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        // 1. add sstables
        let mut key = BytesMut::default();
        key.put_u16(0);
        key.put_slice(b"same_key");
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            Default::default(),
        )
        .await;
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            storage.filter_key_extractor_manager().clone(),
        );
        let worker_node2 = hummock_manager_ref
            .cluster_manager
            .add_worker_node(
                WorkerType::ComputeNode,
                HostAddress::default(),
                Property::default(),
            )
            .await
            .unwrap();
        let _snapshot = hummock_manager_ref
            .pin_snapshot(worker_node2.id)
            .await
            .unwrap();
        let key = key.freeze();
        const SST_COUNT: u64 = 32;
        const TEST_WATERMARK: u64 = 8;
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 10,
            (1..SST_COUNT + 1).map(|v| (v * 1000) << 16).collect_vec(),
        )
        .await;
        // 2. get compact task
        while let Some(mut compact_task) = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
        {
            let compaction_filter_flag = CompactionFilterFlag::TTL;
            compact_task.watermark = (TEST_WATERMARK * 1000) << 16;
            compact_task.compaction_filter_mask = compaction_filter_flag.bits();
            compact_task.table_options = HashMap::from([(
                0,
                TableOption {
                    retention_seconds: 64,
                },
            )]);
            compact_task.current_epoch_time = 0;

            let (_tx, rx) = tokio::sync::oneshot::channel();
            let (mut result_task, task_stats) =
                Compactor::compact(Arc::new(compact_ctx.clone()), compact_task.clone(), rx).await;

            hummock_manager_ref
                .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
                .await
                .unwrap();
        }

        let mut val = b"0"[..].repeat(1 << 10);
        val.extend_from_slice(&((TEST_WATERMARK * 1000) << 16).to_be_bytes());

        let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
        let _recv = compactor_manager.add_compactor(worker_node.id);

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let group =
            version.get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());

        // base level
        let output_tables = group
            .levels
            .iter()
            .flat_map(|level| level.table_infos.clone())
            .chain(
                group
                    .l0
                    .as_ref()
                    .unwrap()
                    .sub_levels
                    .iter()
                    .flat_map(|level| level.table_infos.clone()),
            )
            .collect_vec();

        storage.wait_version(version).await;
        let mut table_key_count = 0;
        for output_sst in output_tables {
            let table = storage
                .sstable_store()
                .sstable(&output_sst, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            table_key_count += table.value().meta.key_count;
        }

        // we have removed these 31 keys before watermark 32.
        assert_eq!(table_key_count, (SST_COUNT - TEST_WATERMARK + 1) as u32);
        let read_epoch = (TEST_WATERMARK * 1000) << 16;

        let get_ret = storage
            .get(
                key.clone(),
                read_epoch,
                ReadOptions {
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await;
        let get_val = get_ret.unwrap().unwrap().to_vec();

        assert_eq!(get_val, val);
        let ret = storage
            .get(
                key.clone(),
                ((TEST_WATERMARK - 1) * 1000) << 16,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: Some(key.clone()),
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    async fn test_compaction_same_key_not_split() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            Default::default(),
        )
        .await;
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            storage.filter_key_extractor_manager().clone(),
        );

        // 1. add sstables with 1MB value
        let mut key = BytesMut::default();
        key.put_u16(0);
        key.put_slice(b"same_key");
        let key = key.freeze();
        const SST_COUNT: u64 = 16;

        let mut val = b"0"[..].repeat(1 << 20);
        val.extend_from_slice(&SST_COUNT.to_be_bytes());
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 20,
            (1..SST_COUNT + 1).collect_vec(),
        )
        .await;

        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::NONE;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.current_epoch_time = 0;

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            SST_COUNT as usize / 2 + 1,
        );
        compact_task.target_level = 6;

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (mut result_task, task_stats) =
            Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        hummock_manager_ref
            .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
            .await
            .unwrap();

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_table = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap()
            .table_infos
            .first()
            .unwrap();
        let table = storage
            .sstable_store()
            .sstable(output_table, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let target_table_size = storage.storage_opts().sstable_size_mb * (1 << 20);

        assert!(
            table.value().meta.estimated_size > target_table_size,
            "table.meta.estimated_size {} <= target_table_size {}",
            table.value().meta.estimated_size,
            target_table_size
        );

        // 5. storage get back the correct kv after compaction
        storage.wait_version(version).await;
        let get_val = storage
            .get(
                key.clone(),
                SST_COUNT + 1,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: None,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();
        assert_eq!(get_val, val);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(6, compact_task.target_level);
    }

    pub(crate) async fn flush_and_commit(
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        epoch: u64,
    ) {
        let ssts = storage
            .seal_and_sync_epoch(epoch)
            .await
            .unwrap()
            .uncommitted_ssts;
        hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
    }

    async fn prepare_data(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        existing_table_id: u32,
        keys_per_epoch: usize,
    ) {
        let kv_count: u16 = 128;
        let mut epoch: u64 = 1;

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
        for idx in 0..kv_count {
            epoch += 1;

            if idx == 0 {
                local.init(epoch);
            }

            for _ in 0..keys_per_epoch {
                let mut key = idx.to_be_bytes().to_vec();
                let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
                key.extend_from_slice(&ramdom_key);
                local.insert(Bytes::from(key), val.clone(), None).unwrap();
            }
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(epoch + 1);

            flush_and_commit(&hummock_meta_client, storage, epoch).await;
        }
    }

    pub(crate) fn prepare_compactor_and_filter(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        existing_table_id: u32,
    ) -> CompactorContext {
        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        get_compactor_context_with_filter_key_extractor_manager(
            storage,
            hummock_meta_client,
            filter_key_extractor_manager,
        )
    }

    #[tokio::test]
    async fn test_compaction_drop_all_key() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id: u32 = 1;
        let storage_existing_table_id = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        prepare_data(
            hummock_meta_client.clone(),
            &storage_existing_table_id,
            existing_table_id,
            1,
        )
        .await;

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[existing_table_id])
            .await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        // 3. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(0, output_level_info.total_file_size);

        // 5. get compact task
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap();

        assert!(compact_task.is_none());
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_existing_table_id() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let global_storage = get_global_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
        )
        .await;

        // register the local_storage to global_storage
        let mut storage_1 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(1)))
            .await;
        let mut storage_2 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(2)))
            .await;

        let filter_key_extractor_manager = global_storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            1,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager_impl(
            global_storage.storage_opts().clone(),
            global_storage.sstable_store(),
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
        );

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value

        let drop_table_id = 1;
        let existing_table_ids = 2;
        let kv_count: usize = 128;
        let mut epoch: u64 = 1;
        register_table_ids_to_compaction_group(
            &hummock_manager_ref,
            &[drop_table_id, existing_table_ids],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        for index in 0..kv_count {
            epoch += 1;
            let next_epoch = epoch + 1;
            if index == 0 {
                storage_1.init(epoch);
                storage_2.init(epoch);
            }

            let (storage, other) = if index % 2 == 0 {
                (&mut storage_1, &mut storage_2)
            } else {
                (&mut storage_2, &mut storage_1)
            };

            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.put_u16(1);
            prefix.put_slice(random_key.as_slice());

            storage.insert(prefix.freeze(), val.clone(), None).unwrap();
            storage.flush(Vec::new()).await.unwrap();
            storage.seal_current_epoch(next_epoch);
            other.seal_current_epoch(next_epoch);

            let ssts = global_storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[drop_table_id]).await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .filter(|level| level.level_idx != compact_task.target_level)
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count
        );

        // 4. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (mut result_task, task_stats) =
            Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        hummock_manager_ref
            .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
            .await
            .unwrap();

        // 5. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += global_storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        assert_eq!((kv_count / 2) as u32, key_count);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        global_storage.wait_version(version).await;

        // 7. scan kv to check key table_id
        let scan_result = global_storage
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                epoch,
                None,
                ReadOptions {
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: TableId::from(existing_table_ids),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_ids);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_retention_seconds() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id = 2;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;
        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
        );
        filter_key_extractor_manager.update(
            2,
            Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        );

        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value

        let kv_count = 11;
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let mut epoch_set = BTreeSet::new();

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        for i in 0..kv_count {
            epoch += millisec_interval_epoch;
            let next_epoch = epoch + millisec_interval_epoch;
            if i == 0 {
                local.init(epoch);
            }
            epoch_set.insert(epoch);
            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.put_u16(1);
            prefix.put_slice(random_key.as_slice());

            local.insert(prefix.freeze(), val.clone(), None).unwrap();
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(next_epoch);

            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        let retention_seconds_expire_second = 1;
        compact_task.table_options = HashMap::from_iter([(
            existing_table_id,
            TableOption {
                retention_seconds: retention_seconds_expire_second,
            },
        )]);
        compact_task.current_epoch_time = epoch;

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (mut result_task, task_stats) =
            Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        hummock_manager_ref
            .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(StaticCompactionGroupId::StateDefault.into(), |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32 - retention_seconds_expire_second + 1;
        assert_eq!(expect_count, key_count); // retention_seconds will clean the key (which epoch < epoch - retention_seconds)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let scan_result = storage
            .scan(
                (Bound::Unbounded, Bound::Unbounded),
                epoch,
                None,
                ReadOptions {
                    ignore_range_tombstone: false,

                    prefix_hint: None,
                    table_id: TableId::from(existing_table_id),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_with_filter_key_extractor() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));

        let existing_table_id = 2;
        let mut key = BytesMut::default();
        key.put_u16(1);
        key.put_slice(b"key_prefix");
        let key_prefix = key.freeze();
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;

        let filter_key_extractor_manager = storage.filter_key_extractor_manager().clone();
        filter_key_extractor_manager.update(
            existing_table_id,
            Arc::new(FilterKeyExtractorImpl::FixedLength(
                FixedLengthFilterKeyExtractor::new(TABLE_PREFIX_LEN + key_prefix.len()),
            )),
        );

        let compact_ctx = get_compactor_context_with_filter_key_extractor_manager(
            &storage,
            &hummock_meta_client,
            filter_key_extractor_manager.clone(),
        );

        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value
        let kv_count = 11;
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let mut epoch_set = BTreeSet::new();

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        for i in 0..kv_count {
            epoch += millisec_interval_epoch;
            if i == 0 {
                local.init(epoch);
            }
            let next_epoch = epoch + millisec_interval_epoch;
            epoch_set.insert(epoch);

            let ramdom_key = [key_prefix.as_ref(), &rand::thread_rng().gen::<[u8; 32]>()].concat();
            local
                .insert(Bytes::from(ramdom_key), val.clone(), None)
                .unwrap();
            local.flush(Vec::new()).await.unwrap();
            local.seal_current_epoch(next_epoch);
            let ssts = storage
                .seal_and_sync_epoch(epoch)
                .await
                .unwrap()
                .uncommitted_ssts;
            hummock_meta_client.commit_epoch(epoch, ssts).await.unwrap();
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // compact_task.table_options =
        //     HashMap::from_iter([(existing_table_id, TableOption { ttl: 0 })]);
        compact_task.current_epoch_time = epoch;

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (mut result_task, task_stats) =
            Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        hummock_manager_ref
            .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let tables_from_version: Vec<_> = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .collect::<Vec<_>>();

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .value()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32;
        assert_eq!(expect_count, key_count); // ttl will clean the key (which epoch < epoch - ttl)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch += 1;
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let bloom_filter_key = [
            existing_table_id.to_be_bytes().to_vec(),
            key_prefix.to_vec(),
        ]
        .concat();
        let start_bound_key = key_prefix;
        let end_bound_key = Bytes::from(next_key(start_bound_key.as_ref()));
        let scan_result = storage
            .scan(
                (
                    Bound::Included(start_bound_key),
                    Bound::Excluded(end_bound_key),
                ),
                epoch,
                None,
                ReadOptions {
                    ignore_range_tombstone: false,
                    prefix_hint: Some(Bytes::from(bloom_filter_key)),
                    table_id: TableId::from(existing_table_id),
                    retention_seconds: None,
                    read_version_from_backup: false,
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();

        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_delete_range() {
        let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
            &hummock_manager_ref,
            TableId::from(existing_table_id),
        )
        .await;
        let compact_ctx =
            prepare_compactor_and_filter(&storage, &hummock_meta_client, existing_table_id);

        prepare_data(hummock_meta_client.clone(), &storage, existing_table_id, 2).await;
        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        local.init(130);
        let prefix_key_range = |k: u16| {
            let key = k.to_be_bytes();
            (
                Bound::Included(Bytes::copy_from_slice(key.as_slice())),
                Bound::Excluded(Bytes::copy_from_slice(next_key(key.as_slice()).as_slice())),
            )
        };
        local
            .flush(vec![prefix_key_range(1u16), prefix_key_range(2u16)])
            .await
            .unwrap();
        local.seal_current_epoch(u64::MAX);

        flush_and_commit(&hummock_meta_client, &storage, 130).await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                manual_compcation_option,
            )
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            129
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let (mut result_task, task_stats) =
            Compactor::compact(Arc::new(compact_ctx), compact_task.clone(), rx).await;

        hummock_manager_ref
            .report_compact_task(&mut result_task, Some(to_prost_table_stats_map(task_stats)))
            .await
            .unwrap();

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
            .levels
            .last()
            .unwrap();
        assert_eq!(1, output_level_info.table_infos.len());
        assert_eq!(252, output_level_info.table_infos[0].total_key_count);
    }

    #[async_trait::async_trait]
    pub trait SstableInfoGenerator {
        async fn generate(
            &mut self,
            sst_id: u64,
            kv_count: usize,
            epoch: u64,
            sstable_store: &SstableStoreRef,
        ) -> SstableInfo;
    }

    fn apply_compaction_task(levels: &mut Levels, compact_task: CompactTask) {
        let mut delete_sst_levels = compact_task
            .input_ssts
            .iter()
            .map(|level| level.level_idx)
            .collect_vec();
        if delete_sst_levels.len() > 1 {
            delete_sst_levels.sort();
            delete_sst_levels.dedup();
        }
        let delete_sst_ids_set: HashSet<u64> = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.sst_id)
            .collect();
        levels.apply_compact_ssts(GroupDeltasSummary {
            delete_sst_levels,
            delete_sst_ids_set,
            insert_sst_level_id: compact_task.target_level,
            insert_sub_level_id: compact_task.target_sub_level_id,
            insert_table_infos: compact_task.sorted_output_ssts,
            group_construct: None,
            group_destroy: None,
            group_meta_changes: vec![],
            group_table_change: None,
        });
    }

    pub struct CompactTest {
        throughput_multiplier: u64,
        group: Levels,
        group_config: CompactionGroup,
        selector: Box<dyn LevelSelector>,
        sstable_store: SstableStoreRef,
        handlers: Vec<LevelHandler>,
        global_task_id: HummockCompactionTaskId,
        pending_tasks: BTreeMap<u64, (CompactTask, u64, u64)>,
        stats: LocalSelectorStatistic,
        global_sst_id: Arc<AtomicU64>,
        rng: ThreadRng,
    }

    impl CompactTest {
        pub fn new(throughput_multiplier: u64) -> Self {
            let mut group = Levels {
                levels: vec![],
                l0: Some(OverlappingLevel::default()),
                group_id: 1,
                parent_group_id: 0,
                member_table_ids: vec![1, 2],
            };
            let mut handlers = vec![LevelHandler::new(0)];
            for idx in 1..7 {
                group.levels.push(Level {
                    level_idx: idx,
                    level_type: 0,
                    table_infos: vec![],
                    total_file_size: 0,
                    sub_level_id: 0,
                    uncompressed_file_size: 0,
                });
                handlers.push(LevelHandler::new(idx));
            }
            Self {
                group_config: CompactionGroup::new(1, CompactionConfigBuilder::new().build()),
                selector: default_level_selector(),
                sstable_store: mock_sstable_store(),
                handlers,
                pending_tasks: BTreeMap::default(),
                global_task_id: 1,
                group,
                global_sst_id: Arc::new(AtomicU64::new(1)),
                stats: LocalSelectorStatistic::default(),
                rng: rand::thread_rng(),
                throughput_multiplier,
            }
        }

        fn pick_one_task(&mut self, start_time: u64) -> bool {
            if let Some(task) = self.selector.pick_compaction(
                self.global_task_id,
                &self.group_config,
                &self.group,
                &mut self.handlers,
                &mut self.stats,
                HashMap::default(),
            ) {
                let mut task: CompactTask = task.into();
                if CompactStatus::is_trivial_move_task(&task) {
                    task.sorted_output_ssts = task.input_ssts[0].table_infos.clone();
                    task.set_task_status(TaskStatus::Success);
                    for level in &task.input_ssts {
                        self.handlers[level.level_idx as usize].remove_task(task.task_id);
                    }
                    apply_compaction_task(&mut self.group, task);
                } else {
                    let task_size = task
                        .input_ssts
                        .iter()
                        .flat_map(|level| level.table_infos.iter())
                        .map(|sst| sst.file_size)
                        .sum::<u64>();
                    let compact_speed = 32 * 1024 * 1024;
                    self.pending_tasks.insert(
                        self.global_task_id,
                        (
                            task,
                            start_time,
                            self.rng.next_u64() % 3 + 1 + task_size / compact_speed,
                        ),
                    );
                }
                self.global_task_id += 1;
                return true;
            }
            false
        }

        async fn test_random_compact_impl<S: SstableInfoGenerator>(&mut self, mut generator: S) {
            const CHECKPOINT_TIMES: u64 = 10;
            const TEST_TIMES: u64 = 100000;
            const KV_COUNT: usize = 16;
            const MAX_COMPACT_TASK_COUNT: usize = 16;
            let mut rng = rand::thread_rng();
            let mut finished_task = vec![];
            for i in 1..TEST_TIMES {
                if i % CHECKPOINT_TIMES == 0 {
                    let mut sst_info = generator
                        .generate(
                            self.global_sst_id.fetch_add(1, Ordering::Relaxed),
                            rng.next_u64() as usize % KV_COUNT + 1,
                            i,
                            &self.sstable_store,
                        )
                        .await;
                    sst_info.uncompressed_file_size *= self.throughput_multiplier;
                    sst_info.file_size *= self.throughput_multiplier;
                    self.group.l0.as_mut().unwrap().sub_levels.push(Level {
                        level_idx: 0,
                        level_type: LevelType::Overlapping as i32,
                        total_file_size: sst_info.file_size,
                        uncompressed_file_size: sst_info.uncompressed_file_size,
                        table_infos: vec![sst_info],
                        sub_level_id: 0,
                    });
                    if self.pending_tasks.len() < MAX_COMPACT_TASK_COUNT {
                        self.pick_one_task(i);
                    }
                }
                for (task_id, (_, start_time, cost_time)) in &self.pending_tasks {
                    if start_time + cost_time <= i {
                        finished_task.push(*task_id);
                    }
                }

                if !finished_task.is_empty() {
                    for task_id in finished_task.drain(..) {
                        let (mut task, _, _) = self.pending_tasks.remove(&task_id).unwrap();
                        self.finish_compaction_task(&mut task).await;
                        apply_compaction_task(&mut self.group, task);
                    }
                    while self.pending_tasks.len() < MAX_COMPACT_TASK_COUNT {
                        if !self.pick_one_task(i) {
                            break;
                        }
                    }
                }
            }
        }

        async fn finish_compaction_task(&mut self, task: &mut CompactTask) {
            let task_progress = Arc::new(TaskProgress::default());
            let mut table_iters = vec![];
            for level in &task.input_ssts {
                // Do not need to filter the table because manager has done it.
                if level.level_type == LevelType::Nonoverlapping as i32 {
                    table_iters.push(ConcatSstableIterator::new(
                        vec![1],
                        level.table_infos.clone(),
                        KeyRange::inf(),
                        self.sstable_store.clone(),
                        task_progress.clone(),
                        0,
                    ));
                } else {
                    for table_info in &level.table_infos {
                        table_iters.push(ConcatSstableIterator::new(
                            vec![1],
                            vec![table_info.clone()],
                            KeyRange::inf(),
                            self.sstable_store.clone(),
                            task_progress.clone(),
                            0,
                        ));
                    }
                }
            }
            let opts = SstableBuilderOptions {
                capacity: (task.target_file_size / self.throughput_multiplier) as usize,
                block_capacity: 1024,
                ..Default::default()
            };
            let builder_factory = LocalTableBuilderFactory::with_sst_id(
                self.global_sst_id.clone(),
                self.sstable_store.clone(),
                opts,
            );
            let is_target_l0_or_lbase =
                task.target_level == 0 || task.target_level == task.base_level;

            let mut sst_builder = CapacitySplitTableBuilder::new(
                builder_factory,
                None,
                is_target_l0_or_lbase,
                task.split_by_state_table,
                task.split_weight_by_vnode,
            );
            let iter = UnorderedMergeIteratorInner::for_compactor(table_iters);
            Compactor::compact_and_build_sst(
                &mut sst_builder,
                Arc::new(CompactionDeleteRanges::default()),
                &TaskConfig {
                    key_range: KeyRange::inf(),
                    gc_delete_keys: false,
                    watermark: task.watermark,
                    is_target_l0_or_lbase,
                    use_block_based_filter: true,
                    ..Default::default()
                },
                Arc::new(CompactorMetrics::unused()),
                iter,
                DummyCompactionFilter,
                None,
            )
            .await
            .unwrap();
            let ret = sst_builder.finish().await.unwrap();
            let mut ssts = Vec::with_capacity(ret.len());
            for mut output in ret {
                output.writer_output.await.unwrap().unwrap();
                output.sst_info.sst_info.file_size *= self.throughput_multiplier;
                output.sst_info.sst_info.uncompressed_file_size *= self.throughput_multiplier;
                ssts.push(output.sst_info.sst_info);
            }
            for level in &task.input_ssts {
                self.handlers[level.level_idx as usize].remove_task(task.task_id);
            }
            task.sorted_output_ssts = ssts;
            task.set_task_status(TaskStatus::Success);
        }
    }

    pub struct RandomGenerator {
        max_pk: u64,
        rng: StdRng,
    }

    pub fn test_table_key_of(idx: u64, vnode: usize) -> TableKey<Vec<u8>> {
        let mut key = VirtualNode::from_index(vnode).to_be_bytes().to_vec();
        key.extend_from_slice(idx.to_be_bytes().as_slice());
        TableKey(key)
    }

    #[async_trait::async_trait]
    impl SstableInfoGenerator for RandomGenerator {
        async fn generate(
            &mut self,
            sst_id: u64,
            kv_count: usize,
            epoch: u64,
            sstable_store: &SstableStoreRef,
        ) -> SstableInfo {
            let writer_opts = SstableWriterOptions {
                capacity_hint: None,
                tracker: None,
                policy: CachePolicy::Disable,
            };
            let opts = SstableBuilderOptions {
                capacity: 1024,
                block_capacity: 256,
                ..Default::default()
            };
            let writer = sstable_store.clone().create_sst_writer(sst_id, writer_opts);
            let mut b = SstableBuilder::<_, BlockedXor16FilterBuilder>::new(
                sst_id,
                writer,
                BlockedXor16FilterBuilder::new(512),
                opts,
                Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
                None,
            );
            let v = epoch.to_be_bytes().to_vec();
            for vnode_idx in 0..VirtualNode::COUNT / 16 {
                let vnode = vnode_idx * 16;
                for _ in 0..kv_count {
                    let k = self.rng.next_u64() % self.max_pk + 1;
                    b.add(
                        FullKey::new(TableId::new(1), test_table_key_of(k, vnode), epoch).to_ref(),
                        HummockValue::Put(v.as_slice()),
                        true,
                    )
                    .await
                    .unwrap();
                }
            }
            let output = b.finish().await.unwrap();
            output.writer_output.await.unwrap().unwrap();
            output.sst_info.sst_info
        }
    }

    #[tokio::test]
    async fn test_random_compact() {
        let mut test = CompactTest::new(512 * 1024);
        test.test_random_compact_impl(RandomGenerator {
            max_pk: 1000000,
            rng: StdRng::seed_from_u64(0),
        })
        .await;
        let mut test = CompactTest::new(1024);
        test.test_random_compact_impl(RandomGenerator {
            max_pk: 1000000,
            rng: StdRng::seed_from_u64(0),
        })
        .await;
    }
}
