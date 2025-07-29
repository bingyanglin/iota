// Test checkpoint retrieval via gRPC
use iota_grpc_api::checkpoint::{
    checkpoint_service_client::CheckpointServiceClient,
    CheckpointStreamRequest,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to gRPC CheckpointService...");
    
    // Try different ports that the node might be using
    let ports = vec![50051, 50052, 50053, 50054];
    
    for port in ports {
        println!("🔍 Trying port {}...", port);
        
        let endpoint = format!("http://127.0.0.1:{}", port);
        match CheckpointServiceClient::connect(endpoint).await {
            Ok(mut client) => {
                println!("✅ Connected to gRPC CheckpointService on port {}!", port);
                
                // Test streaming checkpoints 0 and 1
                println!("📡 Requesting checkpoints 0 and 1...");
                let request = CheckpointStreamRequest {
                    start_index: Some(0),
                    end_index: None,
                    full: Some(false), // Get summary only
                };
                
                match client.stream_checkpoints(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        println!("✅ Successfully subscribed to checkpoint stream!");
                        
                        let mut count = 0;
                        while let Some(checkpoint) = stream.next().await {
                            match checkpoint {
                                Ok(checkpoint) => {
                                    println!("✅ Received checkpoint {}:", checkpoint.index);
                                    if let Some(bcs_data) = &checkpoint.bcs_data {
                                        println!("   BCS data size: {} bytes", bcs_data.data.len());
                                    }
                                    println!("   Is full: {}", checkpoint.is_full);
                                    count += 1;
                                    
                                    if count >= 200 {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    println!("❌ Error receiving checkpoint: {}", e);
                                    break;
                                }
                            }
                        }
                        
                        if count > 0 {
                            println!("🎉 Successfully received {} checkpoints!", count);
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to stream checkpoints: {}", e);
                        continue;
                    }
                }
                
                return Ok(());
            }
            Err(e) => {
                println!("❌ Failed to connect on port {}: {}", port, e);
            }
        }
    }
    
    println!("❌ Could not connect to gRPC service on any port");
    Ok(())
}