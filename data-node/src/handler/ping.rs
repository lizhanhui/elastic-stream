use std::rc::Rc;

use codec::frame::Frame;
use store::ElasticStore;

#[derive(Debug)]
pub(crate) struct Ping {}

impl Ping {
    pub(crate) async fn apply(&self, _store: Rc<ElasticStore>, _response: &mut Frame) {}
}
