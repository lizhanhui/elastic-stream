use super::{composite_session::CompositeSession, lb_policy::LbPolicy};
use crate::{heartbeat::HeartbeatData, naming::Naming};
use log::error;
use model::error::EsError;
use std::{cell::UnsafeCell, collections::HashMap, rc::Rc, sync::Arc};
use tokio::sync::broadcast;

pub struct SessionManager {
    /// Configuration for the transport layer.
    config: Arc<config::Configuration>,

    /// Session management
    sessions: Rc<UnsafeCell<HashMap<String, Rc<CompositeSession>>>>,

    shutdown: broadcast::Sender<()>,
}

impl SessionManager {
    pub(crate) fn new(
        config: &Arc<config::Configuration>,
        shutdown: broadcast::Sender<()>,
    ) -> Self {
        let sessions = Rc::new(UnsafeCell::new(HashMap::new()));
        Self {
            config: Arc::clone(config),
            sessions,
            shutdown,
        }
    }

    /// Broadcast keep-alive heartbeat to all composite sessions managed by current manager.
    pub(crate) async fn broadcast_heartbeat(&self, data: &HeartbeatData) {
        let composite_sessions = unsafe { &mut *self.sessions.get() };
        let composite_sessions = composite_sessions
            .iter()
            .map(|(_k, v)| Rc::clone(v))
            .collect::<Vec<_>>();

        for composite_session in composite_sessions {
            composite_session.heartbeat(data).await;
        }
    }

    pub(crate) async fn get_composite_session(
        &mut self,
        target: &str,
    ) -> Result<Rc<CompositeSession>, EsError> {
        let composite_sessions = unsafe { &mut *self.sessions.get() };
        match composite_sessions.get(target) {
            Some(composite_session) => Ok(Rc::clone(composite_session)),
            None => {
                let composite_session = if target == self.config.placement_driver {
                    let naming = Naming::new(target);
                    let composite_session = CompositeSession::new(
                        naming,
                        Arc::clone(&self.config),
                        LbPolicy::LeaderOnly,
                        self.shutdown.clone(),
                    )
                    .await?;

                    if composite_session
                        .refresh_placement_driver_cluster()
                        .await
                        .is_err()
                    {
                        error!("Failed to refresh placement driver cluster for {target}");
                    }
                    composite_session
                } else {
                    CompositeSession::new(
                        target,
                        Arc::clone(&self.config),
                        LbPolicy::PickFirst,
                        self.shutdown.clone(),
                    )
                    .await?
                };
                let composite_session = Rc::new(composite_session);
                composite_sessions.insert(target.to_owned(), Rc::clone(&composite_session));
                Ok(composite_session)
            }
        }
    }
}
