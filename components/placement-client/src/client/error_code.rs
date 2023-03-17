#[derive(Debug, Clone, PartialEq)]
pub enum ErrorCode {
    Ok = 0,
    Internal = 1,
    InvalidRequest = 2,
    Storage = 3,
    DataNodeNotAvailable = 4,
    NotLeader = 5,
}
