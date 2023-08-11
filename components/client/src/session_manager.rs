use crate::heartbeat::HeartbeatData;

use super::composite_session::CompositeSession;
use log::error;
use model::error::EsError;
use std::{cell::UnsafeCell, collections::HashMap, rc::Rc, sync::Arc};

pub struct SessionManager {
    /// Configuration for the transport layer.
    config: Arc<config::Configuration>,

    /// Session management
    sessions: Rc<UnsafeCell<HashMap<String, Rc<CompositeSession>>>>,
}

impl SessionManager {
    pub(crate) fn new(config: &Arc<config::Configuration>) -> Self {
        let sessions = Rc::new(UnsafeCell::new(HashMap::new()));
        Self {
            config: Arc::clone(config),
            sessions,
        }
    }

    /// Broadcast keep-alive heartbeat to all composite sessions managed by current manager.
    pub(crate) async fn broadcast_heartbeat(&self, data: &HeartbeatData) {
        let map = unsafe { &mut *self.sessions.get() };
        let composite_sessions = map.iter().map(|(_k, v)| Rc::clone(v)).collect::<Vec<_>>();

        for composite_session in composite_sessions {
            composite_session.heartbeat(data).await;
        }
    }

    pub(crate) async fn get_composite_session(
        &mut self,
        target: &str,
    ) -> Result<Rc<CompositeSession>, EsError> {
        let sessions = unsafe { &mut *self.sessions.get() };
        match sessions.get(target) {
            Some(session) => Ok(Rc::clone(session)),
            None => {
                let lb_policy = if target == self.config.placement_driver {
                    super::lb_policy::LbPolicy::LeaderOnly
                } else {
                    super::lb_policy::LbPolicy::PickFirst
                };

                let session = Rc::new(
                    CompositeSession::new(target, Arc::clone(&self.config), lb_policy).await?,
                );

                if lb_policy == super::lb_policy::LbPolicy::LeaderOnly
                    && session.refresh_placement_driver_cluster().await.is_err()
                {
                    error!("Failed to refresh placement driver cluster for {target}");
                }

                sessions.insert(target.to_owned(), Rc::clone(&session));
                Ok(session)
            }
        }
    }
}
