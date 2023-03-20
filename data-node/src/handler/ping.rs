use std::rc::Rc;

use codec::frame::{Frame, OperationCode};
use store::ElasticStore;

#[derive(Debug)]
pub(crate) struct Ping {}

impl Ping {
    pub(crate) async fn apply(&self, store: Rc<ElasticStore>, response: &mut Frame) {
    }
}
