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

#[cfg(test)]
mod tests {
    use protocol::rpc::header::{ClientRole, RangeServerState};

    use super::HeartbeatData;

    #[test]
    fn test_set_state() {
        let mut data = HeartbeatData {
            role: ClientRole::CLIENT_ROLE_RANGE_SERVER,
            state: None,
        };
        data.set_state(RangeServerState::RANGE_SERVER_STATE_OFFLINE);
        assert_eq!(
            Some(RangeServerState::RANGE_SERVER_STATE_OFFLINE),
            data.state
        );
    }
}
