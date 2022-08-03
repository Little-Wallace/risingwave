// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use risingwave_hummock_sdk::{HummockSstableId, SstIdRange};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Mutex;

use crate::hummock::{HummockError, HummockResult};

pub type SstableIdManagerRef = Arc<SstableIdManager>;

/// 1. Caches SST ids fetched from meta.
/// 2. TODO #4037: Maintains watermark SST ids. It will be used by SST full GC.
pub struct SstableIdManager {
    available_sst_ids: Mutex<SstIdRange>,
    pending_fetch_id: AtomicBool,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl SstableIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            available_sst_ids: Mutex::new(SstIdRange::new(
                HummockSstableId::MIN,
                HummockSstableId::MIN,
            )),
            remote_fetch_number,
            hummock_meta_client,
            pending_fetch_id: AtomicBool::new(true),
        }
    }

    pub async fn get_next_sst_id(&self) -> HummockResult<HummockSstableId> {
        let mut guard = self.available_sst_ids.lock().await;
        let available_sst_ids = guard.deref_mut();
        if available_sst_ids.peek_next_sst_id().is_none() {
            drop(available_sst_ids);
            drop(guard);
            if self.pending_fetch_id.load(Ordering::Acquire) {
                let new_sst_ids = self
                    .hummock_meta_client
                    .get_new_sst_ids(1)
                    .await
                    .map_err(HummockError::meta_error)?;
                new_sst_ids.peek_next_sst_id().ok_or_else(|| {
                    HummockError::meta_error("get_new_sst_ids RPC returns empty result")
                })
            } else {
                self.pending_fetch_id.store(true, Ordering::Release);
                let new_sst_ids = self
                    .hummock_meta_client
                    .get_new_sst_ids(self.remote_fetch_number)
                    .await
                    .map_err(HummockError::meta_error)?;
                let mut guard = self.available_sst_ids.lock().await;
                let available_sst_ids = guard.deref_mut();
                if new_sst_ids.start_id < available_sst_ids.end_id {
                    return Err(HummockError::meta_error(format!(
                        "SST id moves backwards. new {} < old {}",
                        new_sst_ids.start_id, available_sst_ids.end_id
                    )));
                }
                *available_sst_ids = new_sst_ids;
                self.pending_fetch_id.store(false, Ordering::Release);
                available_sst_ids.get_next_sst_id().ok_or_else(|| {
                    HummockError::meta_error("get_new_sst_ids RPC returns empty result")
                })
            }
        } else {
            available_sst_ids
                .get_next_sst_id()
                .ok_or_else(|| HummockError::meta_error("get_new_sst_ids RPC returns empty result"))
        }
    }
}
