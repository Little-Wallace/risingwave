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

use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use risingwave_pb::hummock::{group_delta, HummockVersionDelta};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

pub struct CacheRefillPolicy {
    sstable_store: SstableStoreRef,
    metrics: Arc<CompactorMetrics>,
    max_preload_wait_time_mill: u64,
}

impl CacheRefillPolicy {
    pub fn new(
        sstable_store: SstableStoreRef,
        metrics: Arc<CompactorMetrics>,
        max_preload_wait_time_mill: u64,
    ) -> Self {
        Self {
            sstable_store,
            metrics,
            max_preload_wait_time_mill,
        }
    }

    pub async fn execute(self: &Arc<Self>, delta: HummockVersionDelta, max_level: u32) {
        if self.max_preload_wait_time_mill > 0 {
            let policy = self.clone();
            let handle = tokio::spawn(async move {
                let timer = policy.metrics.refill_cache_duration.start_timer();
                let mut ssts = vec![];
                let mut not_in_cache = false;
                for group_delta in delta.group_deltas.values() {
                    if not_in_cache {
                        break;
                    }
                    for d in &group_delta.group_deltas {
                        if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                            d.delta_type.as_ref()
                        {
                            if level_delta.level_idx != 0 {
                                for sst_id in &level_delta.removed_table_ids {
                                    if !policy.sstable_store.contains_sstable(sst_id) {
                                        not_in_cache = true;
                                        break;
                                    }
                                }
                            }
                            ssts.extend(level_delta.inserted_table_infos.clone());
                        }
                    }
                }
                if not_in_cache {
                    return;
                }
                let stats = StoreLocalStatistic::default();
                let mut flatten_reqs = Vec::new();
                for sst in &ssts {
                    flatten_reqs.push(policy.sstable_store.sstable_syncable(sst, &stats));
                }
                policy.metrics.preload_io_count.inc_by(ssts.len() as u64);
                let _ = try_join_all(flatten_reqs).await;
                timer.observe_duration();
            });
            let _ = tokio::time::timeout(
                Duration::from_millis(self.max_preload_wait_time_mill),
                handle,
            )
            .await;
        }
    }
}
