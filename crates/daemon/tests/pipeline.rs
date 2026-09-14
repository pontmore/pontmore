use std::{path::Path, time::Duration};

use nostr::event::Event;
use pontmored::{
    ConnectorManager, Coordinator, DescriptorRegistry, InsertOutcome, OperationStatus, Store,
};
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::broadcast;

#[tokio::test]
async fn signed_history_rebuilds_and_executes_redacted_idempotent_effects() {
    let Ok(database_url) = std::env::var("PONTMORE_TEST_DATABASE_URL") else {
        eprintln!("PONTMORE_TEST_DATABASE_URL is unset; skipping PostgreSQL pipeline test");
        return;
    };
    assert!(
        database_url.ends_with("/pontmore_test"),
        "integration test refuses to truncate a database not named pontmore_test"
    );

    let store = Store::connect(&database_url)
        .await
        .expect("connect test store");
    store.migrate().await.expect("apply migrations");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect cleanup pool");
    sqlx::query(
        "TRUNCATE stream_events, connector_operations, coordination_event_links, \
         coordination_projections, raw_events RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("reset dedicated test database");

    let manifest_config =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/connectors.example.json");
    let connectors = ConnectorManager::load(Some(&manifest_config), Duration::from_secs(2))
        .await
        .expect("load simulated connector");
    let descriptors = DescriptorRegistry::load(Some(&manifest_config))
        .await
        .expect("load descriptor bindings");
    let (stream_sender, _) = broadcast::channel(128);
    let coordinator = Coordinator::new(
        store.clone(),
        connectors.clone(),
        descriptors,
        stream_sender,
    );

    let vectors: Value =
        serde_json::from_str(include_str!("../../../tck/vectors/pip02-swap-v1.json"))
            .expect("parse TCK vectors");
    let input = &vectors["cases"][0]["input"];
    let root: Event = serde_json::from_value(input["root"].clone()).expect("parse root");
    let actions: Vec<Event> =
        serde_json::from_value(input["actions"].clone()).expect("parse actions");
    // Deliver actions in reverse chain order and the root last. Storage must retain
    // them without letting arrival order select protocol state.
    let mut events = actions;
    events.push(root);

    for event in &events {
        coordinator
            .validate_for_ingest(event)
            .expect("event passes context-free validation");
        let (outcome, _) = store.put_event(event).await.expect("store event");
        assert_eq!(outcome, InsertOutcome::Inserted);
        coordinator
            .process_event(event)
            .await
            .expect("rebuild affected coordination");
    }

    let snapshot = store.stream_snapshot().await.expect("load public snapshot");
    assert_eq!(snapshot.coordinations.len(), 1);
    assert_eq!(snapshot.coordinations[0]["state"], "settled");
    assert_eq!(snapshot.connector_operations.len(), 2);
    assert!(
        snapshot
            .connector_operations
            .iter()
            .all(|operation| operation.status == OperationStatus::Succeeded)
    );
    let public_json = serde_json::to_string(&snapshot).expect("serialize public snapshot");
    for private_field in [
        "connector_id",
        "idempotency_key",
        "request",
        "response",
        "error",
    ] {
        assert!(!public_json.contains(private_field));
    }

    let cursor = snapshot.cursor;
    let duplicate = events.last().expect("history has an action");
    let (outcome, stream_event) = store.put_event(duplicate).await.expect("store duplicate");
    assert_eq!(outcome, InsertOutcome::Duplicate);
    assert!(stream_event.is_none());
    coordinator
        .process_event(duplicate)
        .await
        .expect("duplicate rebuild remains safe");
    let after_duplicate = store.stream_snapshot().await.expect("reload snapshot");
    assert_eq!(after_duplicate.cursor, cursor);
    assert_eq!(after_duplicate.connector_operations.len(), 2);

    connectors.shutdown().await;
}
