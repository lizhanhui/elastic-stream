use crate::session_manager::SessionManager;

pub struct Client {
    session_manager: SessionManager,
}

impl Client {
    pub fn create_stream(&self) {
        self.session_manager.create_stream();
    }
}
