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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::HummockEpoch;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::hummock::local_version::local_version_manager::LocalVersionManager;
use crate::hummock::local_version::SyncUncommittedDataStage;
use crate::hummock::shared_buffer::{SharedBufferEvent, WriteRequest};
use crate::hummock::{HummockError, HummockResult, SstableIdManagerRef, TrackerId};
use crate::store::SyncResult;

#[derive(Clone)]
pub(crate) struct BufferTracker {
    flush_threshold: usize,
    block_write_threshold: usize,
    global_buffer_size: Arc<AtomicUsize>,
    pub(crate) global_upload_task_size: Arc<AtomicUsize>,

    pub(crate) buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
}

impl BufferTracker {
    pub fn new(
        flush_threshold: usize,
        block_write_threshold: usize,
        buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
    ) -> Self {
        assert!(
            flush_threshold <= block_write_threshold,
            "flush threshold {} is not less than block write threshold {}",
            flush_threshold,
            block_write_threshold
        );
        info!(
            "buffer tracker init: flush threshold {}, block write threshold {}",
            flush_threshold, block_write_threshold
        );
        Self {
            flush_threshold,
            block_write_threshold,
            global_buffer_size: Arc::new(AtomicUsize::new(0)),
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
            buffer_event_sender,
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer_size.load(Relaxed)
    }

    pub fn get_upload_task_size(&self) -> usize {
        self.global_upload_task_size.load(Relaxed)
    }

    pub fn can_write(&self) -> bool {
        self.get_buffer_size() <= self.block_write_threshold
    }

    pub fn try_write(&self, size: usize) -> bool {
        loop {
            let current_size = self.global_buffer_size.load(Acquire);
            if current_size > self.block_write_threshold {
                return false;
            }
            if self
                .global_buffer_size
                .compare_exchange(current_size, current_size + size, Acquire, Acquire)
                .is_ok()
            {
                break true;
            }
        }
    }

    pub fn send_event(&self, event: SharedBufferEvent) {
        self.buffer_event_sender.send(event).unwrap();
    }
}

pub(crate) struct FlushController {
    local_version_manager: Arc<LocalVersionManager>,
    buffer_tracker: BufferTracker,
    sstable_id_manager: SstableIdManagerRef,
    shared_buffer_event_receiver: mpsc::UnboundedReceiver<SharedBufferEvent>,
    pending_sync_requests: HashMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,
}

impl FlushController {
    pub(crate) fn new(
        local_version_manager: Arc<LocalVersionManager>,
        buffer_tracker: BufferTracker,
        sstable_id_manager: SstableIdManagerRef,
        shared_buffer_event_receiver: mpsc::UnboundedReceiver<SharedBufferEvent>,
    ) -> Self {
        Self {
            local_version_manager,
            buffer_tracker,
            sstable_id_manager,
            shared_buffer_event_receiver,
            pending_sync_requests: Default::default(),
        }
    }

    fn grant_write_request(&mut self, request: WriteRequest) {
        let WriteRequest {
            batch,
            epoch,
            grant_sender: sender,
        } = request;
        let size = batch.size();
        self.local_version_manager
            .write_shared_buffer_inner(epoch, batch);
        self.buffer_tracker
            .global_buffer_size
            .fetch_add(size, Relaxed);
        let _ = sender.send(()).inspect_err(|err| {
            error!("unable to send write request response: {:?}", err);
        });
    }

    fn send_sync_result(&mut self, epoch: HummockEpoch, result: HummockResult<SyncResult>) {
        if let Some(tx) = self.pending_sync_requests.remove(&epoch) {
            let _ = tx.send(result).inspect_err(|e| {
                error!("unable to send sync result. Epoch: {}. Err: {:?}", epoch, e);
            });
        } else {
            panic!("send sync result to non-requested epoch: {}", epoch);
        }
    }
}

// Handler for different events
impl FlushController {
    fn handle_epoch_finished(&mut self, epoch: HummockEpoch) {
        // TODO: in some case we may only need the read guard.
        let mut local_version_guard = self.local_version_manager.local_version.write();
        if epoch > local_version_guard.get_max_sync_epoch() {
            // The finished flush task does not belong to any syncing epoch.
            return;
        }
        let sync_epoch = epoch;
        let sync_data = local_version_guard
            .sync_uncommitted_data
            .get_mut(&sync_epoch)
            .expect("should find");
        match &sync_data.stage {
            SyncUncommittedDataStage::CheckpointEpochSealed(_) => {
                unreachable!("when a join handle is finished, the stage should not be at CheckpointEpochSealed");
            }
            SyncUncommittedDataStage::Syncing(_) => {
                if let Err(e) = &sync_data.ret {
                    let e = Err(HummockError::other(format!("{:?}", e)));
                    drop(local_version_guard);
                    self.send_sync_result(sync_epoch, e);
                } else {
                    unreachable!(
                        "when a join handle is finished, the stage should not be at syncing"
                    );
                }
            }
            SyncUncommittedDataStage::InMemoryMerge(_) => {
                if let Err(e) = &sync_data.ret {
                    let e = Err(HummockError::other(format!("{:?}", e)));
                    drop(local_version_guard);
                    self.send_sync_result(sync_epoch, e);
                } else {
                    unreachable!("when a join handle is finished, the stage should not be at in-memory-merge");
                }
            }
            SyncUncommittedDataStage::Synced(ssts, sync_size) => {
                let ssts = ssts.clone();
                let sync_size = *sync_size;
                drop(local_version_guard);
                self.send_sync_result(
                    sync_epoch,
                    Ok(SyncResult {
                        sync_size,
                        uncommitted_ssts: ssts,
                    }),
                );
            }
        }
    }

    fn handle_buffer_release(&mut self, size: usize) {
        self.buffer_tracker
            .global_buffer_size
            .fetch_sub(size, Relaxed);
        // TODO: notify write thread
    }

    fn handle_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    ) {
        if let Some(old_sync_result_sender) = self
            .pending_sync_requests
            .insert(new_sync_epoch, sync_result_sender)
        {
            let _ = old_sync_result_sender
                .send(Err(HummockError::other(
                    "the sync rx is overwritten by an new rx",
                )))
                .inspect_err(|e| {
                    error!(
                        "unable to send sync result: {}. Err: {:?}",
                        new_sync_epoch, e
                    );
                });
        }
        let mut local_version_guard = self.local_version_manager.local_version.write();
        // no pending flush to wait. Start syncing
        let (payload, epochs, sync_size) = local_version_guard.start_syncing(new_sync_epoch);
        if !payload.is_empty() {
            let local_version_manager = self.local_version_manager.clone();
            tokio::spawn(async move {
                let _ = local_version_manager
                    .run_sync_upload_task(payload, epochs, sync_size)
                    .await;
            });
        }
    }

    async fn handle_clear(&mut self, notifier: oneshot::Sender<()>) {
        // Wait for all ongoing flush to finish.
        // There cannot be any pending write requests since we should only clear
        // shared buffer after all actors stop processing data.
        assert!(self.pending_write_requests.is_empty());
        let pending_epochs = self.pending_sync_requests.keys().cloned().collect_vec();
        pending_epochs.into_iter().for_each(|epoch| {
            self.send_sync_result(
                epoch,
                Err(HummockError::other("the pending sync is cleared")),
            );
        });

        // Clear shared buffer
        let cleaned_epochs = self
            .local_version_manager
            .local_version
            .write()
            .clear_shared_buffer();
        for cleaned_epoch in cleaned_epochs {
            self.sstable_id_manager
                .remove_watermark_sst_id(TrackerId::Epoch(cleaned_epoch));
        }

        // Notify completion of the Clear event.
        notifier.send(()).unwrap();
    }
}

impl FlushController {
    pub(crate) async fn start_flush_controller_worker(mut self) {
        while let Some(event) = self.shared_buffer_event_receiver.recv().await {
            match event {
                SharedBufferEvent::FlushEnd(epoch) => {
                    self.handle_epoch_finished(epoch);
                }
                SharedBufferEvent::BufferRelease(size) => {
                    self.handle_buffer_release(size);
                }
                SharedBufferEvent::SyncEpoch {
                    new_sync_epoch,
                    sync_result_sender,
                } => {
                    self.handle_sync_epoch(new_sync_epoch, sync_result_sender);
                }
                SharedBufferEvent::Clear(notifier) => {
                    self.handle_clear(notifier).await;
                }
                SharedBufferEvent::Shutdown => {
                    info!("buffer tracker shutdown");
                    break;
                }
            }
        }
    }
}
