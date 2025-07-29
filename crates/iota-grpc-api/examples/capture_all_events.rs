// Capture all events to see what's actually happening in the network
use iota_grpc_api::events::{
    AllFilter, EventFilter, EventStreamRequest, event_service_client::EventServiceClient,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to gRPC EventService at http://127.0.0.1:50051...");
    
    let mut client = EventServiceClient::connect("http://127.0.0.1:50051").await?;
    println!("✅ Connected to gRPC EventService!");
    
    // Create AllFilter to capture everything
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::All(AllFilter {})),
    };
    
    println!("📡 Subscribing to ALL events...");
    let request = EventStreamRequest {
        filter: Some(filter),
    };
    
    let response = client.stream_events(request).await?;
    let mut stream = response.into_inner();
    
    println!("⏳ Waiting for events (will capture first 10 events or timeout after 30 seconds)...");
    
    let mut event_count = 0;
    let timeout = tokio::time::timeout(
        tokio::time::Duration::from_secs(300),
        async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        event_count += 1;
                        println!("\n🎉 Event #{}: ", event_count);
                        
                        if let Some(event_id) = &event.event_id {
                            println!("   TX Digest: {}", event_id.tx_digest);
                            println!("   Event Seq: {}", event_id.event_seq);
                        }
                        
                        println!("   Event Data: {} bytes", event.event_data.len());
                        
                        // Try to deserialize the event data to see what's inside
                        if !event.event_data.is_empty() {
                            match bcs::from_bytes::<iota_types::event::Event>(&event.event_data) {
                                Ok(iota_event) => {
                                    println!("   📦 Deserialized Event:");
                                    println!("      Package ID: {}", iota_event.package_id);
                                    println!("      Transaction Module: {}", iota_event.transaction_module);
                                    println!("      Sender: {}", iota_event.sender);
                                    println!("      Type: {}", iota_event.type_);
                                    println!("      Contents: {} bytes", iota_event.contents.len());
                                }
                                Err(e) => {
                                    println!("   ⚠️ Could not deserialize event data: {}", e);
                                    println!("   📄 Raw bytes: {:?}", &event.event_data[..std::cmp::min(100, event.event_data.len())]);
                                }
                            }
                        }
                        
                        if let Some(timestamp_ms) = event.timestamp_ms {
                            println!("   Timestamp: {}", timestamp_ms);
                        }
                        
                        println!("   Raw event: {:?}", event);
                        
                        // Stop after 10 events to avoid spam
                        if event_count >= 10 {
                            break;
                        }
                    }
                    Err(e) => {
                        println!("❌ Error receiving event: {}", e);
                        break;
                    }
                }
            }
        }
    ).await;
    
    match timeout {
        Ok(_) => {
            if event_count > 0 {
                println!("\n✅ Captured {} events successfully!", event_count);
                println!("💡 You can now use the real values above in your filtering examples!");
            } else {
                println!("\n⚠️  No events received. The network might be quiet.");
                println!("💡 Try generating some activity (send transactions, stake, etc.)");
            }
        }
        Err(_) => {
            if event_count > 0 {
                println!("\n⏰ Timeout reached after capturing {} events", event_count);
            } else {
                println!("\n⏰ Timeout reached - no events received in 30 seconds");
                println!("💡 The network might be quiet. Try:");
                println!("   - Send some transactions using the faucet");
                println!("   - Use iota client to interact with the network");
                println!("   - Check if there are any system events being generated");
            }
        }
    }
    
    Ok(())
}