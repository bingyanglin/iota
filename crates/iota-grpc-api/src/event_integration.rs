// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use iota_core::authority::AuthorityState;
use iota_json_rpc_types::{EventFilter, Filter, IotaEvent};
use tokio::sync::broadcast;
use tonic::Status;
use tracing::info;

use crate::EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE;

/// Trait for integrating with IOTA's event subscription system
#[tonic::async_trait]
pub trait EventIntegrationTrait: Send + Sync {
    async fn subscribe(
        &self,
        event_filter: EventFilter,
    ) -> Result<broadcast::Receiver<IotaEvent>, Status>;
}

/// Integrates IOTA's event subscription system with gRPC event service
pub struct EventIntegration {
    // Use shared broadcast channel instead of authority state subscription
    event_broadcast_tx: broadcast::Sender<Arc<IotaEvent>>,
}

impl EventIntegration {
    /// Create a new EventIntegration (legacy method, kept for compatibility)
    pub fn new(_authority_state: Arc<AuthorityState>) -> Self {
        // Create a local broadcast channel as fallback (this won't receive events)
        let (tx, _) = broadcast::channel(EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE);
        Self { event_broadcast_tx: tx }
    }

    /// Create a new EventIntegration with shared broadcast channel
    pub fn new_with_broadcast_channel(event_broadcast_tx: broadcast::Sender<Arc<IotaEvent>>) -> Self {
        Self { event_broadcast_tx }
    }
}

#[tonic::async_trait]
impl EventIntegrationTrait for EventIntegration {
    /// Subscribe to events matching the given filter
    async fn subscribe(
        &self,
        event_filter: EventFilter,
    ) -> Result<broadcast::Receiver<IotaEvent>, Status> {
        info!(
            event_filter = ?event_filter,
            "🔔 EventIntegration::subscribe - New subscription request with filter"
        );

        // Subscribe to the shared broadcast channel
        let mut rx = self.event_broadcast_tx.subscribe();

        // Create a filtered broadcast channel for this specific filter
        let (filtered_tx, filtered_rx) = broadcast::channel(EVENT_INTEGRATION_BROADCAST_BUFFER_SIZE);

        info!(
            "🚀 EventIntegration::subscribe - Created filtered broadcast channel for gRPC client"
        );

        // Create a synchronization channel to ensure receiver is ready
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        // Spawn a task to filter events and forward them to the client
        tokio::spawn(async move {
            info!("🔄 EventIntegration filtering task started, waiting for events from shared channel...");
            
            // Signal that the receiver is ready to start receiving
            let _ = ready_tx.send(());
            
            while let Ok(event_arc) = rx.recv().await {
                let event = (*event_arc).clone();
                
                // Apply filter to the event
                if event_filter.matches(&event) {
                    info!(
                        "📡 EventIntegration received filtered event - TX: {}, Event Seq: {}, Type: {}, Sender: {}",
                        event.id.tx_digest,
                        event.id.event_seq,
                        event.type_.name.as_ident_str(),
                        event.sender
                    );
                    
                    // Forward event to gRPC subscribers
                    match filtered_tx.send(event.clone()) {
                        Ok(subscriber_count) => {
                            info!(
                                subscriber_count = subscriber_count,
                                tx_digest = %event.id.tx_digest,
                                "✅ EventIntegration forwarded filtered event to {} gRPC subscriber(s)",
                                subscriber_count
                            );
                        }
                        Err(_) => {
                            info!(
                                tx_digest = %event.id.tx_digest,
                                "📭 EventIntegration: No gRPC subscribers to forward filtered event to"
                            );
                        }
                    }
                } else {
                    // Event didn't match filter, skip silently
                }
            }
            info!("🔚 EventIntegration filtering task ended - shared channel closed");
        });

        // Wait for the receiver to be ready before returning
        if let Err(_) = ready_rx.await {
            return Err(Status::internal("Failed to initialize event receiver"));
        }

        info!("✅ EventIntegration::subscribe completed successfully - receiver is active");
        Ok(filtered_rx)
    }
}
