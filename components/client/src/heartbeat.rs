use protocol::rpc::header::{ClientRole, RangeServerState};

#[derive(Debug)]
pub struct HeartbeatData {
    /// This is a flag field, indicating if the heartbeat should be directly broadcasted to all servers, regardless of
    /// underlying connection idleness.
    pub mandatory: bool,

    /// Role of the component that sends the heartbeat request.
    pub role: ClientRole,

    /// If the sender is range server, we optionally include current range server state that may be helpful for PD
    /// orchestration of streams.
    pub state: Option<RangeServerState>,
}

impl HeartbeatData {
    /// Create `HeartbeatData` with required fields filled
    pub fn new(role: ClientRole) -> Self {
        Self {
            mandatory: false,
            role,
            state: None,
        }
    }

    pub fn set_state(&mut self, state: RangeServerState) {
        self.state = Some(state);
    }

    pub fn make_mandatory(&mut self) {
        self.mandatory = true;
    }

    #[inline]
    pub fn mandatory(&self) -> bool {
        self.mandatory
    }
}

#[cfg(test)]
mod tests {
    use protocol::rpc::header::{ClientRole, RangeServerState};

    use super::HeartbeatData;

    #[test]
    fn test_set_state() {
        let mut data = HeartbeatData::new(ClientRole::CLIENT_ROLE_RANGE_SERVER);
        data.set_state(RangeServerState::RANGE_SERVER_STATE_OFFLINE);
        assert_eq!(
            Some(RangeServerState::RANGE_SERVER_STATE_OFFLINE),
            data.state
        );
    }
}
