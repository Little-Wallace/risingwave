use std::collections::HashMap;
use risingwave_common::hash::VirtualNode;
use crate::bwtree::TypedPage;

pub struct RootPage {
    vnodes: HashMap<VirtualNode, TypedPage>,
}