use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_object_store::object::MonitoredStreamingUploader;

use crate::bwtree::delta_chain::Delta;
use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::smo::IndexPageRedoLogRecord;
use crate::bwtree::PageId;
use crate::hummock::{CompressionAlgorithm, HummockResult};

pub struct SegmentBuilder {
    algorithm: CompressionAlgorithm,
    writer: MonitoredStreamingUploader,
    page_index: Vec<(PageId, u64, usize)>,
    delta_index: Vec<(PageId, u64, usize)>,
    data_len: usize,
}

impl SegmentBuilder {
    pub async fn append_page(&mut self, page: Arc<LeafPage>) -> HummockResult<()> {
        self.page_index
            .push((page.get_page_id(), page.epoch(), self.data_len));
        let data = page.compress(self.algorithm);
        let mut buf =
            BytesMut::with_capacity(page.encode_size() + std::mem::size_of::<u32>() * 2 + 1);
        let total_size = page.encode_size() + data.len() + std::mem::size_of::<u32>() /* size of data.len() */ + 1;
        buf.put_u32_le(total_size as u32);
        buf.put_u64_le(page.get_page_id());
        buf.put_u64_le(page.get_right_link());
        buf.put_u64_le(page.epoch());
        buf.put_u32_le(page.smallest_user_key.len() as u32);
        buf.put_slice(page.smallest_user_key.as_ref());
        buf.put_u32_le(page.largest_user_key.len() as u32);
        buf.put_slice(page.largest_user_key.as_ref());
        buf.put_u8(self.algorithm.into());
        buf.put_u32_le(data.len() as u32);
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_delta(&mut self, paeg_id: PageId, delta: Arc<Delta>) -> HummockResult<()> {
        self.delta_index
            .push((paeg_id, delta.max_epoch(), self.data_len));
        let data = delta.data();
        let meta_size = std::mem::size_of::<PageId>() + std::mem::size_of::<u64>() * 2;
        let mut buf = BytesMut::with_capacity(meta_size + std::mem::size_of::<u32>());
        buf.put_u32_le((meta_size + data.len()) as u32);
        buf.put_u64_le(paeg_id);
        buf.put_u64_le(delta.max_epoch());
        buf.put_u64_le(delta.prev_epoch());
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_index_page(&mut self, page_id: PageId, data: Bytes) -> HummockResult<()> {
        let total_size = (std::mem::size_of::<PageId>() + data.len()) as u32;
        let mut buf =
            BytesMut::with_capacity(std::mem::size_of::<PageId>() + std::mem::size_of::<u32>());
        buf.put_u32_le(total_size);
        buf.put_u64_le(page_id);
        self.data_len += buf.len();
        self.writer.write_bytes(buf.freeze()).await?;
        self.data_len += data.len();
        self.writer.write_bytes(data).await?;
        Ok(())
    }

    pub async fn append_redo_log(
        &mut self,
        logs: Vec<IndexPageRedoLogRecord>,
    ) -> HummockResult<()> {
    }
}
