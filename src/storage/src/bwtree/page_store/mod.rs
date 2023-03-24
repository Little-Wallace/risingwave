use crate::bwtree::leaf_page::LeafPage;
use crate::bwtree::PageID;
use crate::hummock::HummockResult;

pub struct PageStore {}

impl PageStore {
    pub async fn get_data_page(&self, pid: PageID) -> HummockResult<LeafPage> {
        unimplemented!()
    }
}
