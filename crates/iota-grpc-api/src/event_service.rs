// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use iota_json_rpc_types::EventFilter;
use iota_types::{base_types::{ObjectID, IotaAddress}, digests::TransactionDigest};
use move_core_types::{identifier::Identifier, language_storage::StructTag};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::{
    EVENT_STREAM_BUFFER_SIZE,
    event_integration::EventIntegrationTrait,
    events::{
        Event, EventId, EventStreamRequest, event_service_server::EventService as EventServiceTrait,
    },
};

pub struct EventService {
    event_integration: Box<dyn EventIntegrationTrait>,
}

impl EventService {
    pub fn new(event_integration: impl EventIntegrationTrait + 'static) -> Self {
        Self {
            event_integration: Box::new(event_integration),
        }
    }
}

pub fn create_event_filter(
    proto_filter: &crate::events::EventFilter,
) -> Result<EventFilter, Status> {
    match &proto_filter.filter {
        Some(crate::events::event_filter::Filter::MoveEventType(filter)) => {
            let package_id = ObjectID::from_hex_literal(&filter.address)
                .map_err(|_| Status::invalid_argument("Invalid package ID"))?;

            let struct_tag = StructTag {
                address: (*package_id).into(),
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
                value: serde_json::json!(filter.value),
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
        Some(crate::events::event_filter::Filter::All(_)) => {
            Ok(EventFilter::All(vec![])) // Match all events
        }
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
        None => {
            // Default to matching all events if no filter is specified
            Ok(EventFilter::All(vec![]))
        }
    }
}

// Helper function to build nested AND filters
// Since EventFilter::And only takes 2 filters, we need to nest them for
// multiple filters [A, B, C] becomes And(A, And(B, C))
fn build_and_filter(mut filters: Vec<EventFilter>) -> Result<EventFilter, Status> {
    if filters.is_empty() {
        return Err(Status::invalid_argument("AND filter cannot be empty"));
    }

    if filters.len() == 1 {
        return Ok(filters.into_iter().next().unwrap());
    }

    // Build right-associative nested structure
    let mut result = filters.pop().unwrap();
    while let Some(filter) = filters.pop() {
        result = EventFilter::And(Box::new(filter), Box::new(result));
    }

    Ok(result)
}

// Helper function to build nested OR filters
// Since EventFilter::Or only takes 2 filters, we need to nest them for multiple
// filters [A, B, C] becomes Or(A, Or(B, C))
fn build_or_filter(mut filters: Vec<EventFilter>) -> Result<EventFilter, Status> {
    if filters.is_empty() {
        return Err(Status::invalid_argument("OR filter cannot be empty"));
    }

    if filters.len() == 1 {
        return Ok(filters.into_iter().next().unwrap());
    }

    // Build right-associative nested structure
    let mut result = filters.pop().unwrap();
    while let Some(filter) = filters.pop() {
        result = EventFilter::Or(Box::new(filter), Box::new(result));
    }

    Ok(result)
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
        
        info!("🔗 New GRPC client subscribed to events with filter: {:?}", event_filter);

        let (tx, rx) = mpsc::channel(EVENT_STREAM_BUFFER_SIZE);
        let mut receiver = self.event_integration.subscribe(event_filter).await?;

        tokio::spawn(async move {
            while let Ok(event) = receiver.recv().await {
                info!(
                    "🚀 GRPC Event broadcasted to client - TX: {}, Event Seq: {}, Type: {}, Timestamp: {:?}",
                    event.id.tx_digest,
                    event.id.event_seq,
                    event.type_.name.as_ident_str(),
                    event.timestamp_ms
                );
                
                let proto_event = Event {
                    event_data: bcs::to_bytes(&event).unwrap(),
                    event_id: Some(EventId {
                        tx_seq: event.id.event_seq,
                        event_seq: event.id.event_seq,
                        tx_digest: event.id.tx_digest.to_string(),
                    }),
                    timestamp_ms: event.timestamp_ms,
                };

                if tx.send(Ok(proto_event)).await.is_err() {
                    info!("❌ GRPC Event client disconnected");
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
