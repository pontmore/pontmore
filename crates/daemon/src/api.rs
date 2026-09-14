use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::Event as SseEvent, sse::KeepAlive},
    routing::{get, post},
};
use nostr::event::Event;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tower_http::trace::TraceLayer;

use crate::{
    connector::ConnectorManager,
    store::{InsertOutcome, Store, StoreError, StreamEvent},
};

const STREAM_PAGE_SIZE: i64 = 1_000;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub connectors: ConnectorManager,
    pub stream_sender: broadcast::Sender<StreamEvent>,
}

/// Build the deliberately small client API.
///
/// Signed event submission is the only client-to-daemon data path. SSE is the
/// only daemon-to-client data path. Health endpoints are operational probes.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/events", post(put_event))
        .route("/v1/stream", get(stream))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn live() -> Json<Value> {
    Json(json!({"status": "live"}))
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state.store.ready().await?;
    Ok(Json(json!({"status": "ready"})))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventSubmission {
    event: Event,
}

async fn put_event(
    State(state): State<AppState>,
    Json(input): Json<EventSubmission>,
) -> Result<StatusCode, ApiError> {
    input.event.verify().map_err(|error| {
        ApiError::bad_request(
            "INVALID_EVENT",
            format!("event verification failed: {error}"),
        )
    })?;
    let (outcome, stream_event) = state.store.put_event(&input.event).await?;
    if let Some(stream_event) = stream_event {
        let _ = state.stream_sender.send(stream_event);
    }
    let status = if outcome == InsertOutcome::Inserted {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok(status)
}

async fn stream(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let supplied_cursor = parse_cursor(&headers)?;
    let mut receiver = state.stream_sender.subscribe();
    let initial = if supplied_cursor.is_none() {
        Some(state.store.stream_snapshot().await?)
    } else {
        None
    };
    let connectors = state.connectors.manifests();
    let mut cursor =
        supplied_cursor.unwrap_or_else(|| initial.as_ref().map_or(0, |snapshot| snapshot.cursor));
    let store = state.store;

    let events = async_stream::stream! {
        if let Some(snapshot) = initial {
            let data = json!({
                "coordinations": snapshot.coordinations,
                "connector_operations": snapshot.connector_operations,
                "connectors": connectors,
            });
            yield Ok::<_, Infallible>(sse_event(snapshot.cursor, "snapshot", &data));
        }

        loop {
            match store.stream_events_after(cursor, STREAM_PAGE_SIZE).await {
                Ok(replay) if !replay.is_empty() => {
                    for item in replay {
                        cursor = item.sequence;
                        yield Ok(sse_event(item.sequence, &item.event_type, &item.data));
                    }
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    yield Ok(sse_event(
                        cursor,
                        "stream_error",
                        &json!({"code": "STORAGE_ERROR", "message": error.to_string()}),
                    ));
                    break;
                }
            }

            match receiver.recv().await {
                Ok(item) if item.sequence > cursor => {
                    cursor = item.sequence;
                    yield Ok(sse_event(item.sequence, &item.event_type, &item.data));
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    // The durable replay at the top of the loop closes gaps.
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Ok(Sse::new(events).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn parse_cursor(headers: &HeaderMap) -> Result<Option<i64>, ApiError> {
    let Some(value) = headers.get("last-event-id") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        ApiError::bad_request("INVALID_STREAM_CURSOR", "Last-Event-ID must be an integer")
    })?;
    let cursor = value.parse::<i64>().map_err(|_| {
        ApiError::bad_request("INVALID_STREAM_CURSOR", "Last-Event-ID must be an integer")
    })?;
    if cursor < 0 {
        return Err(ApiError::bad_request(
            "INVALID_STREAM_CURSOR",
            "Last-Event-ID cannot be negative",
        ));
    }
    Ok(Some(cursor))
}

fn sse_event(sequence: i64, event_type: &str, data: &Value) -> SseEvent {
    SseEvent::default()
        .id(sequence.to_string())
        .event(event_type)
        .json_data(data)
        .expect("serializing serde_json::Value cannot fail")
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorBody {
                code,
                message: message.into(),
            },
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::EventCollision | StoreError::IdempotencyConflict => Self {
                status: StatusCode::CONFLICT,
                body: ErrorBody {
                    code: "CONFLICT",
                    message: error.to_string(),
                },
            },
            StoreError::Database(_) | StoreError::Migration(_) | StoreError::Json(_) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorBody {
                    code: "STORAGE_ERROR",
                    message: "storage operation failed".to_owned(),
                },
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CACHE_CONTROL, "no-store")],
            Json(self.body),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_optional_and_nonnegative() {
        assert_eq!(parse_cursor(&HeaderMap::new()).expect("valid"), None);
        let mut headers = HeaderMap::new();
        headers.insert("last-event-id", "42".parse().expect("header"));
        assert_eq!(parse_cursor(&headers).expect("valid"), Some(42));
        headers.insert("last-event-id", "-1".parse().expect("header"));
        assert!(parse_cursor(&headers).is_err());
    }
}
