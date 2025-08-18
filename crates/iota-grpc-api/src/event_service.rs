// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{str::FromStr, sync::Arc};

use iota_json_rpc_types::{EventFilter, Filter, IotaEvent};
use iota_types::{
    base_types::{IotaAddress, ObjectID},
    digests::TransactionDigest,
};
use move_core_types::{identifier::Identifier, language_storage::StructTag};
use serde_json;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::{
    EVENT_STREAM_BUFFER_SIZE,
    events::{
        Event, EventId, EventStreamRequest, event_service_server::EventService as EventServiceTrait,
    },
};

pub struct EventService {
    event_sender: broadcast::Sender<Arc<IotaEvent>>,
}

impl EventService {
    pub fn new(event_sender: broadcast::Sender<Arc<IotaEvent>>) -> Self {
        Self { event_sender }
    }
}

#[tonic::async_trait]
impl EventServiceTrait for EventService {
    type StreamEventsStream = ReceiverStream<Result<Event, Status>>;

    async fn stream_events(
        &self,
        request: Request<EventStreamRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let proto_filter = request
            .into_inner()
            .filter
            .ok_or_else(|| Status::invalid_argument("Filter is required"))?;

        let event_filter = create_event_filter(&proto_filter)?;
        debug!("New gRPC client subscribed with filter: {:?}", event_filter);

        // Create new receiver for this subscription
        let mut event_rx = self.event_sender.subscribe();
        let (tx, rx) = mpsc::channel(EVENT_STREAM_BUFFER_SIZE);

        tokio::spawn(async move {
            while let Ok(event_arc) = event_rx.recv().await {
                let event = &*event_arc;

                // Use existing filter matching logic
                if event_filter.matches(event) {
                    debug!(
                        "Event matched filter: TX: {}, Type: {}, Sender: {}",
                        event.id.tx_digest,
                        event.type_.name.as_ident_str(),
                        event.sender
                    );

                    // Direct BCS serialization - no conversion needed!
                    let proto_event = Event {
                        event_data: bcs::to_bytes(event).unwrap(),
                        event_id: Some(EventId {
                            tx_seq: event.id.event_seq,
                            event_seq: event.id.event_seq,
                            tx_digest: event.id.tx_digest.to_string(),
                        }),
                        timestamp_ms: event.timestamp_ms,
                    };

                    if tx.send(Ok(proto_event)).await.is_err() {
                        debug!("gRPC client disconnected");
                        break;
                    }
                }
            }
            debug!("Event streaming task ended for gRPC client");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Convert protobuf EventFilter to iota_json_rpc_types::EventFilter
pub fn create_event_filter(
    proto_filter: &crate::events::EventFilter,
) -> Result<EventFilter, Status> {
    match &proto_filter.filter {
        Some(crate::events::event_filter::Filter::MoveEventType(filter)) => {
            let package_id = ObjectID::from_hex_literal(&filter.address)
                .map_err(|_| Status::invalid_argument("Invalid package ID"))?;

            let struct_tag = StructTag {
                address: *package_id,
                module: Identifier::from_str(&filter.module)
                    .map_err(|_| Status::invalid_argument("Invalid module name"))?,
                name: Identifier::from_str(&filter.name)
                    .map_err(|_| Status::invalid_argument("Invalid event name"))?,
                type_params: vec![],
            };
            Ok(EventFilter::MoveEventType(struct_tag))
        }
        Some(crate::events::event_filter::Filter::MoveEventField(filter)) => {
            Ok(EventFilter::MoveEventField {
                path: filter.path.clone(),
                value: serde_json::Value::String(filter.value.clone()),
            })
        }
        Some(crate::events::event_filter::Filter::Package(filter)) => {
            let package_id = ObjectID::from_hex_literal(&filter.package_id)
                .map_err(|_| Status::invalid_argument("Invalid package ID"))?;
            Ok(EventFilter::Package(package_id))
        }
        Some(crate::events::event_filter::Filter::MoveEventModule(filter)) => {
            let package_id = ObjectID::from_hex_literal(&filter.package_id)
                .map_err(|_| Status::invalid_argument("Invalid package ID"))?;
            let module = Identifier::from_str(&filter.module)
                .map_err(|_| Status::invalid_argument("Invalid module name"))?;
            Ok(EventFilter::MoveEventModule {
                package: package_id,
                module,
            })
        }
        Some(crate::events::event_filter::Filter::And(and_filter)) => {
            let converted_filters: Result<Vec<_>, _> =
                and_filter.filters.iter().map(create_event_filter).collect();
            let filters = converted_filters?;
            build_and_filter(filters)
        }
        Some(crate::events::event_filter::Filter::Or(or_filter)) => {
            let converted_filters: Result<Vec<_>, _> =
                or_filter.filters.iter().map(create_event_filter).collect();
            let filters = converted_filters?;
            build_or_filter(filters)
        }
        Some(crate::events::event_filter::Filter::All(_)) => Ok(EventFilter::All(vec![])),
        Some(crate::events::event_filter::Filter::Sender(filter)) => {
            let sender = IotaAddress::from_str(&filter.sender)
                .map_err(|_| Status::invalid_argument("Invalid sender address"))?;
            Ok(EventFilter::Sender(sender))
        }
        Some(crate::events::event_filter::Filter::Transaction(filter)) => {
            let tx_digest = TransactionDigest::from_str(&filter.tx_digest)
                .map_err(|_| Status::invalid_argument("Invalid transaction digest"))?;
            Ok(EventFilter::Transaction(tx_digest))
        }
        Some(crate::events::event_filter::Filter::MoveModule(filter)) => {
            let package_id = ObjectID::from_hex_literal(&filter.package_id)
                .map_err(|_| Status::invalid_argument("Invalid package ID"))?;
            let module = Identifier::from_str(&filter.module)
                .map_err(|_| Status::invalid_argument("Invalid module name"))?;
            Ok(EventFilter::MoveModule {
                package: package_id,
                module,
            })
        }
        Some(crate::events::event_filter::Filter::TimeRange(filter)) => {
            Ok(EventFilter::TimeRange {
                start_time: filter.start_time,
                end_time: filter.end_time,
            })
        }
        None => Ok(EventFilter::All(vec![])),
    }
}

/// Build AND filter by chaining EventFilter::And
fn build_and_filter(mut filters: Vec<EventFilter>) -> Result<EventFilter, Status> {
    if filters.is_empty() {
        return Err(Status::invalid_argument("AND filter cannot be empty"));
    }
    if filters.len() == 1 {
        return Ok(filters.into_iter().next().unwrap());
    }

    let mut result = filters.pop().unwrap();
    while let Some(filter) = filters.pop() {
        result = EventFilter::And(Box::new(filter), Box::new(result));
    }
    Ok(result)
}

/// Build OR filter by chaining EventFilter::Or
fn build_or_filter(mut filters: Vec<EventFilter>) -> Result<EventFilter, Status> {
    if filters.is_empty() {
        return Err(Status::invalid_argument("OR filter cannot be empty"));
    }
    if filters.len() == 1 {
        return Ok(filters.into_iter().next().unwrap());
    }

    let mut result = filters.pop().unwrap();
    while let Some(filter) = filters.pop() {
        result = EventFilter::Or(Box::new(filter), Box::new(result));
    }
    Ok(result)
}
