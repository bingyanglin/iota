use std::{sync::Arc, time::Instant};

use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    proto::iota::gprc::v1::{
        GetSystemInfoRequest, StringU64, SubscribeSystemEventsRequest, SystemEventGprc,
        SystemInfoGprc, system_gprc_service_server::SystemGprcService,
    },
    server::StateReader,
};

const NODE_VERSION: &str = "iota-gprc-api-dev";
const SYSTEM_EVENT_CHANNEL_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct SystemServiceImpl {
    #[allow(dead_code)]
    state_reader: StateReader,
    start_time: Arc<Instant>,
    system_event_sender: broadcast::Sender<SystemEventGprc>,
}

impl SystemServiceImpl {
    pub fn new(state_reader: StateReader, app_start_time: Arc<Instant>) -> Self {
        let (event_tx, _event_rx) = broadcast::channel(SYSTEM_EVENT_CHANNEL_CAPACITY);

        let mock_event_sender = event_tx.clone();
        tokio::spawn(async move {
            let mut tick_count: u64 = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                tick_count += 1;

                let event = SystemEventGprc {
                    event_type: crate::proto::iota::gprc::v1::system_event_gprc::EventType::NodeStatusChanged as i32,
                    details_json: format!(r#"{{"status": "OK", "tick": {}}} "#, tick_count),
                    timestamp_ms: Some(StringU64 {
                        value: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis()
                            .to_string(),
                    }),
                };
                if mock_event_sender.send(event).is_err() {
                    println!(
                        "[SystemServiceMockEventGenerator] No active subscribers for system event. Generator continues."
                    );
                }
            }
        });

        Self {
            state_reader,
            start_time: app_start_time,
            system_event_sender: event_tx,
        }
    }
}

#[tonic::async_trait]
impl SystemGprcService for SystemServiceImpl {
    async fn get_system_info(
        &self,
        request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<SystemInfoGprc>, Status> {
        println!(
            "[gRPC SystemService] Received GetSystemInfo request: {:?}",
            request.get_ref()
        );

        let uptime_duration = self.start_time.elapsed();
        let uptime_ms = uptime_duration.as_millis();

        let system_info = SystemInfoGprc {
            node_version: NODE_VERSION.to_string(),
            uptime_ms: Some(StringU64 {
                value: uptime_ms.to_string(),
            }),
        };

        Ok(Response::new(system_info))
    }

    type SubscribeSystemEventsStream = ReceiverStream<Result<SystemEventGprc, Status>>;

    async fn subscribe_system_events(
        &self,
        request: Request<SubscribeSystemEventsRequest>,
    ) -> Result<Response<Self::SubscribeSystemEventsStream>, Status> {
        println!(
            "[gRPC SystemService] Received SubscribeSystemEvents request: {:?}",
            request.get_ref()
        );

        let mut receiver = self.system_event_sender.subscribe();

        let (tx, rx) = tokio::sync::mpsc::channel(SYSTEM_EVENT_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        if tx.send(Ok(event)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped_count)) => {
                        eprintln!(
                            "[gRPC SystemService] System event stream lagged, skipped {} messages.",
                            skipped_count
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
