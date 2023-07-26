use protocol::rpc::header::{ClientRole, RangeServerState};

#[derive(Debug)]
pub struct HeartbeatData {
    pub role: ClientRole,
    pub state: Option<RangeServerState>,
}

impl HeartbeatData {
    pub fn set_state(&mut self, state: RangeServerState) {
        self.state = Some(state);
    }
}
