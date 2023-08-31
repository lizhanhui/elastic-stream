#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Unspecified,
    Connecting,
    Active,
    GoingAway,
    Closed,
}
