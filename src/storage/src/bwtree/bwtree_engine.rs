use risingwave_common::catalog::TableId;
use crate::bwtree::root_page::RootPage;

pub struct Enine {
    table_id: TableId,
    page: RootPage,
}

pub struct EnineFactory {
}