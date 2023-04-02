use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::PageId;
use crate::hummock::HummockResult;

pub struct PageStore {}

impl PageStore {
    pub async fn get_data_page(&self, pid: PageId) -> HummockResult<LeafPage> {
        unimplemented!()
    }
}
