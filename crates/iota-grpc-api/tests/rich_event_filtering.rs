// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Tests for rich event filtering with AND/OR logic

use std::str::FromStr;

use iota_grpc_api::{
    event_integration::EventIntegrationTrait,
    event_service::EventService,
    events::{
        AllFilter, AndFilter, EventFilter, MoveEventFieldFilter, MoveEventModuleFilter,
        MoveEventTypeFilter, MoveModuleFilter, OrFilter, PackageFilter, SenderFilter,
        TimeRangeFilter, TransactionFilter,
    },
};
use iota_json_rpc_types::{EventFilter as JsonEventFilter, IotaEvent};
use iota_types::{base_types::{ObjectID, IotaAddress}, digests::TransactionDigest};
use move_core_types::identifier::Identifier;
use tokio::sync::broadcast;
use tonic::Status;

// Mock EventIntegration for testing
#[derive(Clone)]
struct MockEventIntegration {
    events: Vec<IotaEvent>,
}

impl MockEventIntegration {
    fn new() -> Self {
        Self { events: vec![] }
    }

    fn with_events(events: Vec<IotaEvent>) -> Self {
        Self { events }
    }
}

#[tonic::async_trait]
impl EventIntegrationTrait for MockEventIntegration {
    async fn subscribe(
        &self,
        _event_filter: JsonEventFilter,
    ) -> Result<broadcast::Receiver<IotaEvent>, Status> {
        let (tx, rx) = broadcast::channel(100);

        // Send mock events that match the filter
        for event in &self.events {
            if let Err(_) = tx.send(event.clone()) {
                break;
            }
        }

        Ok(rx)
    }
}

// Helper to create a test package ID
fn test_package_id() -> ObjectID {
    ObjectID::from_hex_literal("0x1234567890abcdef1234567890abcdef12345678").unwrap()
}

// Helper to create a test sender address
fn test_sender_address() -> IotaAddress {
    // Create a deterministic address for testing
    static SENDER: std::sync::OnceLock<IotaAddress> = std::sync::OnceLock::new();
    *SENDER.get_or_init(|| IotaAddress::random_for_testing_only())
}

// Helper to create a test transaction digest
fn test_transaction_digest() -> TransactionDigest {
    // Create a deterministic digest for testing
    static DIGEST: std::sync::OnceLock<TransactionDigest> = std::sync::OnceLock::new();
    *DIGEST.get_or_init(|| TransactionDigest::random())
}

#[tokio::test]
async fn test_move_event_type_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveEventType(
            MoveEventTypeFilter {
                address: test_package_id().to_string(),
                module: "request".to_string(),
                name: "RequestEvent".to_string(),
            },
        )),
    };

    // Create event service
    let event_integration = MockEventIntegration::new();
    let _service = EventService::new(event_integration);

    // The filter should be convertible to JsonEventFilter
    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::MoveEventType(struct_tag)) = json_filter {
        assert_eq!(struct_tag.address, test_package_id().into());
        assert_eq!(struct_tag.module, Identifier::from_str("request").unwrap());
        assert_eq!(
            struct_tag.name,
            Identifier::from_str("RequestEvent").unwrap()
        );
    } else {
        panic!("Expected MoveEventType filter");
    }
}

#[tokio::test]
async fn test_move_event_field_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveEventField(
            MoveEventFieldFilter {
                path: "anchor".to_string(),
                value: "test_anchor_id".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::MoveEventField { path, value }) = json_filter {
        assert_eq!(path, "anchor");
        assert_eq!(value, serde_json::json!("test_anchor_id"));
    } else {
        panic!("Expected MoveEventField filter");
    }
}

#[tokio::test]
async fn test_and_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::And(
            AndFilter {
                filters: vec![
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveEventType(
                            MoveEventTypeFilter {
                                address: test_package_id().to_string(),
                                module: "request".to_string(),
                                name: "RequestEvent".to_string(),
                            },
                        )),
                    },
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveEventField(
                            MoveEventFieldFilter {
                                path: "anchor".to_string(),
                                value: "test_anchor_id".to_string(),
                            },
                        )),
                    },
                ],
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    // Should create a nested AND filter
    if let Ok(JsonEventFilter::And(box_filter1, box_filter2)) = json_filter {
        // First filter should be MoveEventType
        assert!(matches!(*box_filter1, JsonEventFilter::MoveEventType(_)));

        // Second filter should be MoveEventField
        assert!(matches!(
            *box_filter2,
            JsonEventFilter::MoveEventField { .. }
        ));
    } else {
        panic!("Expected And filter, got: {:?}", json_filter);
    }
}

#[tokio::test]
async fn test_or_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Or(OrFilter {
            filters: vec![
                EventFilter {
                    filter: Some(iota_grpc_api::events::event_filter::Filter::Package(
                        PackageFilter {
                            package_id: test_package_id().to_string(),
                        },
                    )),
                },
                EventFilter {
                    filter: Some(iota_grpc_api::events::event_filter::Filter::MoveEventField(
                        MoveEventFieldFilter {
                            path: "status".to_string(),
                            value: "completed".to_string(),
                        },
                    )),
                },
            ],
        })),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    // Should create a nested OR filter
    if let Ok(JsonEventFilter::Or(box_filter1, box_filter2)) = json_filter {
        // First filter should be Package
        assert!(matches!(*box_filter1, JsonEventFilter::Package(_)));

        // Second filter should be MoveEventField
        assert!(matches!(
            *box_filter2,
            JsonEventFilter::MoveEventField { .. }
        ));
    } else {
        panic!("Expected Or filter, got: {:?}", json_filter);
    }
}

#[tokio::test]
async fn test_complex_nested_filter() {
    // Test: (Package AND (Field1 OR Field2))
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::And(
            AndFilter {
                filters: vec![
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::Package(
                            PackageFilter {
                                package_id: test_package_id().to_string(),
                            },
                        )),
                    },
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::Or(OrFilter {
                            filters: vec![
                                EventFilter {
                                    filter: Some(
                                        iota_grpc_api::events::event_filter::Filter::MoveEventField(
                                            MoveEventFieldFilter {
                                                path: "anchor".to_string(),
                                                value: "anchor1".to_string(),
                                            },
                                        ),
                                    ),
                                },
                                EventFilter {
                                    filter: Some(
                                        iota_grpc_api::events::event_filter::Filter::MoveEventField(
                                            MoveEventFieldFilter {
                                                path: "status".to_string(),
                                                value: "urgent".to_string(),
                                            },
                                        ),
                                    ),
                                },
                            ],
                        })),
                    },
                ],
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    // Should create a nested structure: And(Package, Or(Field1, Field2))
    if let Ok(JsonEventFilter::And(box_filter1, box_filter2)) = json_filter {
        assert!(matches!(*box_filter1, JsonEventFilter::Package(_)));
        assert!(matches!(*box_filter2, JsonEventFilter::Or(_, _)));
    } else {
        panic!("Expected complex nested filter, got: {:?}", json_filter);
    }
}

#[tokio::test]
async fn test_all_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::All(
            AllFilter {},
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::All(filters)) = json_filter {
        assert!(filters.is_empty());
    } else {
        panic!("Expected All filter");
    }
}

#[tokio::test]
async fn test_package_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Package(
            PackageFilter {
                package_id: test_package_id().to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::Package(package_id)) = json_filter {
        assert_eq!(package_id, test_package_id());
    } else {
        panic!("Expected Package filter");
    }
}

#[tokio::test]
async fn test_move_event_module_filter() {
    let filter = EventFilter {
        filter: Some(
            iota_grpc_api::events::event_filter::Filter::MoveEventModule(MoveEventModuleFilter {
                package_id: test_package_id().to_string(),
                module: "request".to_string(),
            }),
        ),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::MoveEventModule { package, module }) = json_filter {
        assert_eq!(package, test_package_id());
        assert_eq!(module, Identifier::from_str("request").unwrap());
    } else {
        panic!("Expected MoveEventModule filter");
    }
}

#[tokio::test]
async fn test_empty_and_filter_error() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::And(
            AndFilter { filters: vec![] },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}

#[tokio::test]
async fn test_empty_or_filter_error() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Or(OrFilter {
            filters: vec![],
        })),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}

#[tokio::test]
async fn test_invalid_package_id() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Package(
            PackageFilter {
                package_id: "invalid_hex".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}

#[tokio::test]
async fn test_sender_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Sender(
            SenderFilter {
                sender: test_sender_address().to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::Sender(sender)) = json_filter {
        assert_eq!(sender, test_sender_address());
    } else {
        panic!("Expected Sender filter");
    }
}

#[tokio::test]
async fn test_transaction_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Transaction(
            TransactionFilter {
                tx_digest: test_transaction_digest().to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::Transaction(digest)) = json_filter {
        assert_eq!(digest, test_transaction_digest());
    } else {
        panic!("Expected Transaction filter");
    }
}

#[tokio::test]
async fn test_move_module_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveModule(
            MoveModuleFilter {
                package_id: test_package_id().to_string(),
                module: "request".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::MoveModule { package, module }) = json_filter {
        assert_eq!(package, test_package_id());
        assert_eq!(module, Identifier::from_str("request").unwrap());
    } else {
        panic!("Expected MoveModule filter");
    }
}

#[tokio::test]
async fn test_time_range_filter() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::TimeRange(
            TimeRangeFilter {
                start_time: 1640995200000, // 2022-01-01 00:00:00 UTC
                end_time: 1641081600000,   // 2022-01-02 00:00:00 UTC
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    if let Ok(JsonEventFilter::TimeRange { start_time, end_time }) = json_filter {
        assert_eq!(start_time, 1640995200000);
        assert_eq!(end_time, 1641081600000);
    } else {
        panic!("Expected TimeRange filter");
    }
}

#[tokio::test]
async fn test_complex_filter_with_new_types() {
    // Test: (Sender AND (TimeRange OR Transaction))
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::And(
            AndFilter {
                filters: vec![
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::Sender(
                            SenderFilter {
                                sender: test_sender_address().to_string(),
                            },
                        )),
                    },
                    EventFilter {
                        filter: Some(iota_grpc_api::events::event_filter::Filter::Or(OrFilter {
                            filters: vec![
                                EventFilter {
                                    filter: Some(
                                        iota_grpc_api::events::event_filter::Filter::TimeRange(
                                            TimeRangeFilter {
                                                start_time: 1640995200000,
                                                end_time: 1641081600000,
                                            },
                                        ),
                                    ),
                                },
                                EventFilter {
                                    filter: Some(
                                        iota_grpc_api::events::event_filter::Filter::Transaction(
                                            TransactionFilter {
                                                tx_digest: test_transaction_digest().to_string(),
                                            },
                                        ),
                                    ),
                                },
                            ],
                        })),
                    },
                ],
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_ok());

    // Should create a nested structure: And(Sender, Or(TimeRange, Transaction))
    if let Ok(JsonEventFilter::And(box_filter1, box_filter2)) = json_filter {
        assert!(matches!(*box_filter1, JsonEventFilter::Sender(_)));
        assert!(matches!(*box_filter2, JsonEventFilter::Or(_, _)));
    } else {
        panic!("Expected complex nested filter with new types, got: {:?}", json_filter);
    }
}

#[tokio::test]
async fn test_invalid_sender_address() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Sender(
            SenderFilter {
                sender: "invalid_address".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}

#[tokio::test]
async fn test_invalid_transaction_digest() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::Transaction(
            TransactionFilter {
                tx_digest: "invalid_digest".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}

#[tokio::test]
async fn test_invalid_move_module_package_id() {
    let filter = EventFilter {
        filter: Some(iota_grpc_api::events::event_filter::Filter::MoveModule(
            MoveModuleFilter {
                package_id: "invalid_hex".to_string(),
                module: "request".to_string(),
            },
        )),
    };

    let json_filter = iota_grpc_api::event_service::create_event_filter(&filter);
    assert!(json_filter.is_err());
}
