mod tests {
    use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

    use iota_grpc_api::proto::iota::gprc::v1::{
        GetCheckpointRequest, StreamedCheckpoint, SubscribeNewCheckpointsRequest,
        checkpoint_gprc_service_server::{CheckpointGprcService, CheckpointGprcServiceServer},
        streamed_checkpoint,
    };

    #[tonic::async_trait]
    impl CheckpointGprcService for MockCheckpointService {
        type SubscribeNewCheckpointsStream =
            tokio_stream::wrappers::ReceiverStream<Result<StreamedCheckpoint, Status>>;

        async fn get_checkpoint_full(
            // ... existing code ...
        }

 