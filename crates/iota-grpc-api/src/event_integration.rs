// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use iota_json_rpc_types::{EventFilter, Filter, IotaEvent};
use tokio::sync::broadcast;
use tonic::Status;
use tracing::debug;

use crate::EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE;

#[tonic::async_trait]
pub trait EventIntegrationTrait: Send + Sync {
    async fn subscribe(
        &self,
        event_filter: EventFilter,
    ) -> Result<broadcast::Receiver<IotaEvent>, Status>;
}

pub struct EventIntegration {
    event_broadcast_tx: broadcast::Sender<Arc<IotaEvent>>,
}

impl Default for EventIntegration {
    fn default() -> Self {
        Self::new()
    }
}

impl EventIntegration {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE);
        Self {
            event_broadcast_tx: tx,
        }
    }

    pub fn new_with_broadcast_channel(
        event_broadcast_tx: broadcast::Sender<Arc<IotaEvent>>,
    ) -> Self {
        Self { event_broadcast_tx }
    }
}

#[tonic::async_trait]
impl EventIntegrationTrait for EventIntegration {
    async fn subscribe(
        &self,
        event_filter: EventFilter,
    ) -> Result<broadcast::Receiver<IotaEvent>, Status> {
        debug!(
            event_filter = ?event_filter,
            "EventIntegration::subscribe - New subscription request with filter"
        );

        let mut rx = self.event_broadcast_tx.subscribe();
        let (filtered_tx, filtered_rx) =
            broadcast::channel(EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE);

        debug!("EventIntegration::subscribe - Created filtered broadcast channel for gRPC client");

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            debug!(
                "EventIntegration filtering task started, waiting for events from shared channel..."
            );

            let _ = ready_tx.send(());

            while let Ok(event_arc) = rx.recv().await {
                let event = (*event_arc).clone();

                if event_filter.matches(&event) {
                    debug!(
                        "EventIntegration received filtered event - TX: {}, Event Seq: {}, Type: {}, Sender: {}",
                        event.id.tx_digest,
                        event.id.event_seq,
                        event.type_.name.as_ident_str(),
                        event.sender
                    );

                    match filtered_tx.send(event.clone()) {
                        Ok(subscriber_count) => {
                            debug!(
                                subscriber_count = subscriber_count,
                                tx_digest = %event.id.tx_digest,
                                "EventIntegration forwarded filtered event to {} gRPC subscriber(s)",
                                subscriber_count
                            );
                        }
                        Err(_) => {
                            debug!(
                                tx_digest = %event.id.tx_digest,
                                "EventIntegration: No gRPC subscribers to forward filtered event to"
                            );
                        }
                    }
                }
            }
            debug!("EventIntegration filtering task ended - shared channel closed");
        });

        if (ready_rx.await).is_err() {
            return Err(Status::internal("Failed to initialize event receiver"));
        }

        debug!("EventIntegration::subscribe completed successfully - receiver is active");
        Ok(filtered_rx)
    }
}
