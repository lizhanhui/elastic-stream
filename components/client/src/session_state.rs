/// State of the session.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum SessionState {
    /// Session is active and there is no planned peer closing.
    Active,

    /// Server has notified that they are scheduled to shutdown in the near future.
    GoAway,
}
