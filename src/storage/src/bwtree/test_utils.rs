use bytes::{Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{StateTableKey, TableKey};

use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::storage_value::StorageValue;

pub fn from_slice_key(key: &[u8], epoch: u64) -> StateTableKey<Bytes> {
    StateTableKey::new(TableKey(Bytes::copy_from_slice(key)), epoch)
}

pub fn generate_data(key: &[u8], value: &[u8]) -> (Bytes, StorageValue) {
    let k = Bytes::copy_from_slice(key);
    let v = if value.is_empty() {
        StorageValue::new(None)
    } else {
        StorageValue::new(Some(Bytes::copy_from_slice(value)))
    };
    (k, v)
}

pub fn get_key_with_partition(vnode_id: usize, key: &[u8]) -> Bytes {
    let mut k = BytesMut::with_capacity(key.len() + VirtualNode::SIZE);
    k.extend_from_slice(&VirtualNode::from_index(vnode_id).to_be_bytes());
    k.extend_from_slice(key);
    k.freeze()
}

pub fn generate_data_with_partition(
    vnode_id: usize,
    key: &[u8],
    value: &[u8],
) -> (Bytes, StorageValue) {
    let k = get_key_with_partition(vnode_id, key);
    let v = if value.is_empty() {
        StorageValue::new(None)
    } else {
        StorageValue::new(Some(Bytes::copy_from_slice(value)))
    };
    (k, v)
}

pub fn build_shared_buffer_batch(
    data: Vec<(Bytes, StorageValue)>,
    epoch: u64,
) -> SharedBufferBatch {
    let items = SharedBufferBatch::build_shared_buffer_item_batches(data);
    let sz = SharedBufferBatch::measure_batch_size(&items);
    SharedBufferBatch::build_shared_buffer_batch(epoch, items, sz, vec![], TableId::new(1), None)
}
