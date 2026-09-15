use std::{
    convert::Infallible,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_stream::stream;
use axum::{
    Form, Router,
    body::Body,
    extract::{Query, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode,
        header::{self, SET_COOKIE},
    },
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response, Sse, sse::Event as SseEvent},
    routing::{get, post},
};
use futures_util::Stream;
use hmac::{Hmac, Mac};
use maud::{DOCTYPE, Markup, html};
use nostr::event::Event;
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

use crate::{
    Config, ConnectorManager, Coordinator,
    store::{
        InsertOutcome, OperatorConnector, OperatorCoordination, OperatorDashboard, OperatorEvent,
        OperatorOperation, OperatorPublication, OperatorRelay, OperatorWork, Store, StreamEvent,
    },
};

const COOKIE_NAME: &str = "pontmore_ui_session";
const STYLE: &str = include_str!("../assets/ui/style.css");
const SCRIPT: &str = include_str!("../assets/ui/app.js");

#[derive(Clone)]
pub struct UiState {
    store: Store,
    coordinator: Coordinator,
    connectors: ConnectorManager,
    config: UiRuntimeConfig,
    updates: broadcast::Sender<StreamEvent>,
    shutdown: watch::Receiver<bool>,
    shutdown_sender: watch::Sender<bool>,
    password: Option<Arc<str>>,
    sessions: SessionSigner,
    version: &'static str,
}

#[derive(Clone)]
pub struct UiRuntimeConfig {
    client_listen: String,
    ui_listen: String,
    relays: Vec<String>,
    connector_config: Option<String>,
    connector_timeout_ms: u64,
    relay_lookback_secs: u64,
    work_lease_secs: u64,
    stream_max_replay: i64,
    stream_retention_secs: u64,
    require_connector_sandbox: bool,
}

impl From<&Config> for UiRuntimeConfig {
    fn from(config: &Config) -> Self {
        Self {
            client_listen: config.listen.to_string(),
            ui_listen: config.ui_listen.to_string(),
            relays: config.relays.clone(),
            connector_config: config
                .connector_config
                .as_ref()
                .map(|path| path.display().to_string()),
            connector_timeout_ms: config.connector_timeout_ms,
            relay_lookback_secs: config.relay_lookback_secs,
            work_lease_secs: config.work_lease_secs,
            stream_max_replay: config.stream_max_replay,
            stream_retention_secs: config.stream_retention_secs,
            require_connector_sandbox: config.require_connector_sandbox,
        }
    }
}

#[derive(Clone)]
pub struct UiControls {
    coordinator: Coordinator,
    connectors: ConnectorManager,
    config: UiRuntimeConfig,
    shutdown: watch::Sender<bool>,
}

impl UiControls {
    #[must_use]
    pub fn new(
        config: &Config,
        coordinator: Coordinator,
        connectors: ConnectorManager,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        Self {
            coordinator,
            connectors,
            config: UiRuntimeConfig::from(config),
            shutdown,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UiError {
    #[error("could not read operator UI password file: {0}")]
    PasswordFile(#[source] std::io::Error),
    #[error("operator UI password file must be a regular file")]
    PasswordNotFile,
    #[error("operator UI password file must not be readable or writable by group or others")]
    PasswordPermissions,
    #[error("operator UI password file is empty")]
    EmptyPassword,
    #[error("PONTMORE_UI_SESSION_TTL_SECS must be greater than zero")]
    ZeroSessionTtl,
}

impl UiState {
    /// Build state for the private operator listener.
    ///
    /// A missing password enables passwordless operation. The daemon only permits
    /// that mode on a loopback listener.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero session lifetime or an unreadable, empty, or
    /// insufficiently restricted password file.
    pub fn new(
        store: Store,
        updates: broadcast::Sender<StreamEvent>,
        controls: UiControls,
        password_file: Option<&Path>,
        session_ttl: Duration,
        secure_cookie: bool,
    ) -> Result<Self, UiError> {
        if session_ttl.is_zero() {
            return Err(UiError::ZeroSessionTtl);
        }
        let password = password_file.map(read_password).transpose()?.map(Arc::from);
        Ok(Self {
            store,
            coordinator: controls.coordinator,
            connectors: controls.connectors,
            config: controls.config,
            updates,
            shutdown: controls.shutdown.subscribe(),
            shutdown_sender: controls.shutdown,
            password,
            sessions: SessionSigner::new(session_ttl, secure_cookie),
            version: env!("CARGO_PKG_VERSION"),
        })
    }
}

pub fn router(state: UiState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/events", get(events_page).post(submit_event))
        .route("/configuration", get(configuration_page))
        .route("/configuration/relays", post(connect_relay))
        .route("/operations", get(operations_page))
        .route("/controls/execution/pause", post(pause_execution))
        .route("/controls/execution/resume", post(resume_execution))
        .route("/controls/rebuild", post(rebuild_projections))
        .route("/controls/retry-work", post(retry_work))
        .route("/controls/retry-publications", post(retry_publications))
        .route("/controls/probe-connectors", post(probe_connectors))
        .route("/controls/shutdown", post(shutdown_daemon))
        .route("/login", get(login_page).post(login))
        .route("/logout", post(logout))
        .route("/ui/stream", get(dashboard_stream))
        .route("/assets/operator-v1.css", get(style))
        .route("/assets/operator-v1.js", get(script))
        .layer(middleware::from_fn(browser_security))
        .with_state(state)
}

#[derive(Default, Deserialize)]
struct NoticeQuery {
    notice: Option<String>,
}

async fn dashboard(
    State(state): State<UiState>,
    Query(notice): Query<NoticeQuery>,
    headers: HeaderMap,
) -> Response {
    let Some(session) = authenticated_session(&state, &headers) else {
        return Redirect::to("/login").into_response();
    };
    match state.store.operator_dashboard().await {
        Ok(snapshot) => Html(
            page(
                "Pontmore",
                &app_page(
                    "overview",
                    &render_overview(&snapshot),
                    notice_message(notice.notice.as_deref()),
                    &session.csrf,
                    state.version,
                ),
            )
            .into_string(),
        )
        .into_response(),
        Err(error) => {
            tracing::error!(%error, "operator dashboard query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "operator dashboard unavailable",
            )
                .into_response()
        }
    }
}

async fn events_page(
    State(state): State<UiState>,
    Query(notice): Query<NoticeQuery>,
    headers: HeaderMap,
) -> Response {
    render_section(&state, &headers, "events", notice, |snapshot, csrf| {
        events_content(snapshot, csrf, "", None)
    })
    .await
}

async fn configuration_page(
    State(state): State<UiState>,
    Query(notice): Query<NoticeQuery>,
    headers: HeaderMap,
) -> Response {
    render_section(
        &state,
        &headers,
        "configuration",
        notice,
        |snapshot, csrf| configuration_content(snapshot, &state.config, csrf, "", None),
    )
    .await
}

async fn operations_page(
    State(state): State<UiState>,
    Query(notice): Query<NoticeQuery>,
    headers: HeaderMap,
) -> Response {
    render_section(&state, &headers, "operations", notice, |snapshot, csrf| {
        operations_content(snapshot, csrf)
    })
    .await
}

async fn render_section(
    state: &UiState,
    headers: &HeaderMap,
    active: &str,
    notice: NoticeQuery,
    content: impl FnOnce(&OperatorDashboard, &str) -> Markup,
) -> Response {
    let Some(session) = authenticated_session(state, headers) else {
        return Redirect::to("/login").into_response();
    };
    match state.store.operator_dashboard().await {
        Ok(snapshot) => Html(
            page(
                "Pontmore",
                &app_page(
                    active,
                    &content(&snapshot, &session.csrf),
                    notice_message(notice.notice.as_deref()),
                    &session.csrf,
                    state.version,
                ),
            )
            .into_string(),
        )
        .into_response(),
        Err(error) => operator_error(error),
    }
}

#[derive(Deserialize)]
struct CsrfInput {
    csrf: String,
}

#[derive(Deserialize)]
struct RelayInput {
    csrf: String,
    relay_url: String,
}

async fn connect_relay(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<RelayInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    let url = match normalize_relay_url(&input.relay_url) {
        Ok(url) => url,
        Err(error) => return render_relay_error(&state, &headers, &input.relay_url, error).await,
    };
    if state
        .config
        .relays
        .iter()
        .any(|startup| normalize_relay_url(startup).is_ok_and(|value| value == url))
    {
        return Redirect::to("/configuration?notice=relay_already_configured").into_response();
    }
    match state.store.add_operator_relay(&url).await {
        Ok(true) => Redirect::to("/configuration?notice=relay_saved").into_response(),
        Ok(false) => Redirect::to("/configuration?notice=relay_already_configured").into_response(),
        Err(error) => control_error(error),
    }
}

fn normalize_relay_url(input: &str) -> Result<String, &'static str> {
    let input = input.trim();
    if input.is_empty() || input.len() > 2_048 {
        return Err("Enter a relay URL of at most 2048 characters.");
    }
    let mut url = url::Url::parse(input).map_err(|_| "Enter a valid WebSocket relay URL.")?;
    let host = url.host_str().ok_or("The relay URL needs a host.")?;
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1");
    if url.scheme() != "wss" && !(url.scheme() == "ws" && loopback) {
        return Err("Use wss:// for remote relays; ws:// is only allowed on loopback.");
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("Relay URLs cannot contain credentials or fragments.");
    }
    url.set_fragment(None);
    Ok(url.into())
}

async fn render_relay_error(
    state: &UiState,
    headers: &HeaderMap,
    input: &str,
    error: &str,
) -> Response {
    let Some(session) = authenticated_session(state, headers) else {
        return Redirect::to("/login").into_response();
    };
    match state.store.operator_dashboard().await {
        Ok(snapshot) => (
            StatusCode::BAD_REQUEST,
            Html(
                page(
                    "Pontmore",
                    &app_page(
                        "configuration",
                        &configuration_content(
                            &snapshot,
                            &state.config,
                            &session.csrf,
                            input,
                            Some(error),
                        ),
                        None,
                        &session.csrf,
                        state.version,
                    ),
                )
                .into_string(),
            ),
        )
            .into_response(),
        Err(store_error) => operator_error(store_error),
    }
}

#[derive(Deserialize)]
struct EventInput {
    csrf: String,
    event: String,
}

#[derive(Deserialize)]
struct ShutdownInput {
    csrf: String,
    confirm: String,
}

async fn submit_event(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<EventInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    let event = match serde_json::from_str::<Event>(&input.event) {
        Ok(event) => event,
        Err(error) => {
            return render_event_error(&state, &headers, &input.event, &error.to_string()).await;
        }
    };
    if let Err(error) = state.coordinator.validate_for_ingest(&event) {
        return render_event_error(&state, &headers, &input.event, &error.to_string()).await;
    }
    let outcome = match state.store.put_event(&event).await {
        Ok((outcome, stream_event)) => {
            if let Some(stream_event) = stream_event {
                let _ = state.updates.send(stream_event);
            }
            outcome
        }
        Err(error) => {
            return render_event_error(&state, &headers, &input.event, &error.to_string()).await;
        }
    };
    if event.kind.as_u16() == 30_361 {
        if let Err(error) = state.coordinator.process_event(&event).await {
            return render_event_error(&state, &headers, &input.event, &error.to_string()).await;
        }
    }
    let notice = match outcome {
        InsertOutcome::Inserted => "event_accepted",
        InsertOutcome::Duplicate => "event_duplicate",
    };
    Redirect::to(&format!("/events?notice={notice}")).into_response()
}

async fn render_event_error(
    state: &UiState,
    headers: &HeaderMap,
    input: &str,
    error: &str,
) -> Response {
    let Some(session) = authenticated_session(state, headers) else {
        return Redirect::to("/login").into_response();
    };
    tracing::warn!(%error, "operator event submission rejected");
    match state.store.operator_dashboard().await {
        Ok(snapshot) => (
            StatusCode::BAD_REQUEST,
            Html(
                page(
                    "Pontmore",
                    &app_page(
                        "events",
                        &events_content(&snapshot, &session.csrf, input, Some(error)),
                        None,
                        &session.csrf,
                        state.version,
                    ),
                )
                .into_string(),
            ),
        )
            .into_response(),
        Err(store_error) => operator_error(store_error),
    }
}

async fn pause_execution(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    control_result(
        state.store.set_execution_enabled(false).await,
        "/configuration?notice=execution_paused",
    )
}

async fn resume_execution(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    if let Err(error) = state.store.set_execution_enabled(true).await {
        return control_error(error);
    }
    control_result(
        state.coordinator.enqueue_all().await,
        "/configuration?notice=execution_resumed",
    )
}

async fn rebuild_projections(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    control_result(
        state.coordinator.enqueue_all().await,
        "/operations?notice=rebuild_queued",
    )
}

async fn retry_work(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    control_result(
        state.store.retry_failed_work().await,
        "/operations?notice=work_requeued",
    )
}

async fn retry_publications(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    control_result(
        state.store.retry_failed_publications().await,
        "/operations?notice=publications_requeued",
    )
}

async fn probe_connectors(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<CsrfInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    for health in state.connectors.health_all().await {
        if let Err(error) = state.store.record_connector_health(&health).await {
            return control_error(error);
        }
    }
    Redirect::to("/operations?notice=connectors_probed").into_response()
}

async fn shutdown_daemon(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<ShutdownInput>,
) -> Response {
    if let Some(response) = authorize_form(&state, &headers, &input.csrf) {
        return response;
    }
    if input.confirm != "yes" {
        return (StatusCode::BAD_REQUEST, "shutdown confirmation is required").into_response();
    }
    let response = Html(
        page(
            "Shutting down",
            &html! { main class="login" { section class="login-card" {
                h1 { "pontmored is shutting down" }
                p class="muted" { "This page will stop updating when the daemon exits." }
            } } },
        )
        .into_string(),
    )
    .into_response();
    let _ = state.shutdown_sender.send(true);
    response
}

fn authorize_form(state: &UiState, headers: &HeaderMap, csrf: &str) -> Option<Response> {
    let Some(session) = authenticated_session(state, headers) else {
        return Some(StatusCode::UNAUTHORIZED.into_response());
    };
    if !constant_time_equal(session.csrf.as_bytes(), csrf.as_bytes()) {
        return Some((StatusCode::FORBIDDEN, "invalid CSRF token").into_response());
    }
    None
}

fn control_result<T, E: std::fmt::Display>(result: Result<T, E>, target: &'static str) -> Response {
    match result {
        Ok(_) => Redirect::to(target).into_response(),
        Err(error) => control_error(error),
    }
}

fn control_error(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "operator control failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "operator control failed").into_response()
}

fn operator_error(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "operator view query failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "operator view unavailable",
    )
        .into_response()
}

async fn login_page(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if state.password.is_none() || authenticated_session(&state, &headers).is_some() {
        return Redirect::to("/").into_response();
    }
    Html(page("Sign in", &login_form(None)).into_string()).into_response()
}

#[derive(Deserialize)]
struct LoginInput {
    password: String,
}

async fn login(State(state): State<UiState>, Form(input): Form<LoginInput>) -> Response {
    let Some(expected) = &state.password else {
        return Redirect::to("/").into_response();
    };
    if !constant_time_equal(expected.as_bytes(), input.password.as_bytes()) {
        tokio::time::sleep(Duration::from_millis(250)).await;
        return (
            StatusCode::UNAUTHORIZED,
            Html(page("Sign in", &login_form(Some("Invalid password"))).into_string()),
        )
            .into_response();
    }
    let mut response = Redirect::to("/").into_response();
    if let Ok(cookie) = HeaderValue::from_str(&state.sessions.issue()) {
        response.headers_mut().insert(SET_COOKIE, cookie);
    }
    response
}

#[derive(Deserialize)]
struct LogoutInput {
    csrf: String,
}

async fn logout(
    State(state): State<UiState>,
    headers: HeaderMap,
    Form(input): Form<LogoutInput>,
) -> Response {
    let Some(session) = authenticated_session(&state, &headers) else {
        return Redirect::to("/login").into_response();
    };
    if !constant_time_equal(session.csrf.as_bytes(), input.csrf.as_bytes()) {
        return (StatusCode::FORBIDDEN, "invalid CSRF token").into_response();
    }
    let mut response = Redirect::to("/login").into_response();
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_static(
            "pontmore_ui_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
        ),
    );
    response
}

async fn dashboard_stream(State(state): State<UiState>, headers: HeaderMap) -> Response {
    let Some(session) = authenticated_session(&state, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    Sse::new(stream_dashboard(state, session.expires))
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

fn stream_dashboard(
    state: UiState,
    session_expires: Option<u64>,
) -> impl Stream<Item = Result<SseEvent, Infallible>> + Send + 'static {
    stream! {
        let mut updates = state.updates.subscribe();
        let mut shutdown = state.shutdown;
        let refresh_period = Duration::from_secs(5);
        let mut refresh = tokio::time::interval_at(
            tokio::time::Instant::now() + refresh_period,
            refresh_period,
        );
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if session_expires.is_some_and(|expires| expires < now()) {
                break;
            }
            match state.store.operator_dashboard().await {
                Ok(snapshot) => yield Ok(SseEvent::default()
                    .event("dashboard")
                    .data(render_overview(&snapshot).into_string())),
                Err(error) => {
                    tracing::error!(%error, "operator dashboard stream query failed");
                    yield Ok(SseEvent::default().event("dashboard").data(
                        html! { div class="error" { "Dashboard data is temporarily unavailable." } }.into_string()
                    ));
                }
            }
            tokio::select! {
                _ = refresh.tick() => {},
                update = updates.recv() => match update {
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {},
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = shutdown.changed() => break,
            }
        }
    }
}

async fn style() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        STYLE,
    )
}

async fn script() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        SCRIPT,
    )
}

async fn browser_security(request: Request<Body>, next: Next) -> Response {
    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) && !same_origin(request.headers())
    {
        return (StatusCode::FORBIDDEN, "cross-origin request rejected").into_response();
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}

fn same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
    {
        if !matches!(site, "same-origin" | "none") {
            return false;
        }
    }
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return true;
    };
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    url::Url::parse(origin).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && &url[url::Position::BeforeHost..url::Position::AfterPort] == host
    })
}

fn authenticated_session(state: &UiState, headers: &HeaderMap) -> Option<Session> {
    if state.password.is_none() {
        return Some(Session {
            csrf: String::new(),
            expires: None,
        });
    }
    state.sessions.verify(cookie(headers, COOKIE_NAME)?)
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(key, value)| (key == name).then_some(value))
}

fn read_password(path: &Path) -> Result<String, UiError> {
    let metadata = fs::metadata(path).map_err(UiError::PasswordFile)?;
    if !metadata.is_file() {
        return Err(UiError::PasswordNotFile);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(UiError::PasswordPermissions);
        }
    }
    let password = fs::read_to_string(path)
        .map_err(UiError::PasswordFile)?
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    if password.is_empty() {
        return Err(UiError::EmptyPassword);
    }
    Ok(password)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

#[derive(Clone)]
struct SessionSigner {
    key: Arc<[u8; 32]>,
    ttl: Duration,
    secure_cookie: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct Session {
    csrf: String,
    expires: Option<u64>,
}

impl SessionSigner {
    fn new(ttl: Duration, secure_cookie: bool) -> Self {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut key = [0_u8; 32];
        key[..16].copy_from_slice(first.as_bytes());
        key[16..].copy_from_slice(second.as_bytes());
        Self {
            key: Arc::new(key),
            ttl,
            secure_cookie,
        }
    }

    fn issue(&self) -> String {
        let expires = now().saturating_add(self.ttl.as_secs());
        let csrf = Uuid::new_v4().simple().to_string();
        let payload = format!("{expires}.{csrf}");
        let value = format!("{payload}.{}", self.signature(&payload));
        let secure = if self.secure_cookie { "; Secure" } else { "" };
        format!(
            "{COOKIE_NAME}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
            self.ttl.as_secs(),
            secure
        )
    }

    fn verify(&self, value: &str) -> Option<Session> {
        let (payload, supplied_signature) = value.rsplit_once('.')?;
        let (expires, csrf) = payload.split_once('.')?;
        let expires = expires.parse::<u64>().ok()?;
        if expires < now() || csrf.len() != 32 {
            return None;
        }
        let expected_signature = self.signature(payload);
        if !constant_time_equal(expected_signature.as_bytes(), supplied_signature.as_bytes()) {
            return None;
        }
        Some(Session {
            csrf: csrf.to_owned(),
            expires: Some(expires),
        })
    }

    fn signature(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_ref())
            .expect("HMAC accepts keys of any size");
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn page(title: &str, content: &Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                meta name="color-scheme" content="dark";
                title { (title) " · pontmored" }
                link rel="stylesheet" href="/assets/operator-v1.css";
                script src="/assets/operator-v1.js" defer {}
            }
            body { (content) }
        }
    }
}

fn login_form(error: Option<&str>) -> Markup {
    html! {
        main class="login" { section class="login-card" {
            div class="brand" { strong { "Pontmore" } span class="muted" { "operator" } }
            h1 { "Sign in" }
            p class="muted" { "Use the password configured for this daemon." }
            @if let Some(error) = error { p class="error" { (error) } }
            form method="post" action="/login" {
                div class="field" {
                    label for="password" { "Password" }
                    input id="password" name="password" type="password" autocomplete="current-password" required autofocus;
                }
                button class="button" type="submit" { "Sign in" }
            }
        } }
    }
}

fn app_page(
    active: &str,
    content: &Markup,
    notice: Option<&str>,
    csrf: &str,
    version: &str,
) -> Markup {
    html! {
        header class="app-header" {
            div class="shell topbar" {
                a class="brand" href="/" { strong { "Pontmore" } span { "daemon " (version) } }
                div class="toolbar" {
                    span id="connection-status" class="connection" { "Connected" }
                    @if !csrf.is_empty() { form method="post" action="/logout" {
                        input type="hidden" name="csrf" value=(csrf);
                        button class="button ghost" type="submit" { "Sign out" }
                    } }
                }
            }
            nav class="shell navigation" aria-label="Operator" {
                (nav_link("/", "Overview", active == "overview"))
                (nav_link("/events", "Events", active == "events"))
                (nav_link("/operations", "Operations", active == "operations"))
                (nav_link("/configuration", "Configuration", active == "configuration"))
            }
        }
        main class="shell app-content" {
            @if let Some(notice) = notice { div class="notice" role="status" { (notice) } }
            @if active == "overview" { div id="dashboard" { (content) } }
            @else { (content) }
        }
    }
}

fn nav_link(href: &str, label: &str, active: bool) -> Markup {
    let class = if active {
        "nav-link active"
    } else {
        "nav-link"
    };
    html! { a class=(class) href=(href) { (label) } }
}

fn notice_message(notice: Option<&str>) -> Option<&'static str> {
    match notice {
        Some("event_accepted") => Some("Signed event accepted and queued for processing."),
        Some("event_duplicate") => Some("That signed event was already stored."),
        Some("execution_paused") => Some("Automatic economic execution is paused."),
        Some("execution_resumed") => {
            Some("Execution resumed; coordinations were queued for review.")
        }
        Some("rebuild_queued") => Some("All coordination projections were queued for rebuild."),
        Some("work_requeued") => Some("Eligible failed work was requeued."),
        Some("publications_requeued") => Some("Failed publications were requeued."),
        Some("connectors_probed") => Some("Connector health probes completed."),
        Some("relay_saved") => Some(
            "Relay saved. The daemon is connecting and backfilling it now; it will reconnect after a restart.",
        ),
        Some("relay_already_configured") => Some("That relay is already configured."),
        _ => None,
    }
}

fn render_overview(snapshot: &OperatorDashboard) -> Markup {
    let pending_work =
        status_count(&snapshot.work, "pending") + status_count(&snapshot.work, "leased");
    let failed_work = status_count(&snapshot.work, "failed");
    let failed_publications = status_count(&snapshot.publications, "failed");
    let healthy_connectors = snapshot
        .connectors
        .iter()
        .filter(|connector| connector.status == "healthy")
        .count();
    let relay_warning = snapshot
        .relays
        .iter()
        .any(|relay| relay.last_error.is_some());
    let relay_status = if snapshot.relays.is_empty() {
        "Not configured"
    } else if relay_warning {
        "Needs attention"
    } else if snapshot.relays.iter().all(|relay| relay.backfill_complete) {
        "Ready"
    } else {
        "Backfilling"
    };
    let connector_warning = healthy_connectors < snapshot.connectors.len();
    let connector_status = if snapshot.connectors.is_empty() {
        "Not configured".to_owned()
    } else {
        format!("{healthy_connectors}/{} healthy", snapshot.connectors.len())
    };
    html! {
        div class="page-heading" {
            div { p class="eyebrow" { "pontmored" } h1 { "Overview" } }
            span class=(if snapshot.execution_enabled { "badge good" } else { "badge bad" }) {
                @if snapshot.execution_enabled { "Execution enabled" } @else { "Execution paused" }
            }
        }
        section class="grid" {
            (metric("Protocol events", snapshot.event_count))
            (metric("Coordinations", snapshot.coordination_count))
            (metric("Pending work", pending_work))
            (metric("Healthy connectors", healthy_connectors))
        }
        section class="panel" {
            div class="panel-header" { h2 { "System status" } span class="muted" { "Stream " (snapshot.stream_cursor) } }
            div class="status-list" {
                (status_item("Economic execution", if snapshot.execution_enabled { "Enabled" } else { "Paused" }, !snapshot.execution_enabled))
                (status_item("Work queue", if failed_work > 0 { "Needs attention" } else if pending_work > 0 { "Processing" } else { "Idle" }, failed_work > 0))
                (status_item("Publications", if failed_publications > 0 { "Needs attention" } else { "Ready" }, failed_publications > 0))
                (status_item("Relays", relay_status, relay_warning))
                (status_item("Connectors", &connector_status, connector_warning))
            }
        }
        @if snapshot.coordinations.is_empty() {
            section class="empty-state" {
                h2 { "No coordination activity yet" }
                p { "Submit a signed Pontmore event or configure relays to begin receiving protocol activity." }
                div class="empty-actions" {
                    a class="button primary" href="/events" { "Submit an event" }
                    a class="button" href="/configuration" { "Review configuration" }
                }
            }
        } @else { (coordination_panel(&snapshot.coordinations)) }
        p class="timestamp" { "Updated " (time(i64::try_from(snapshot.generated_at).unwrap_or(i64::MAX))) }
    }
}

fn status_count(counts: &std::collections::BTreeMap<String, i64>, status: &str) -> i64 {
    counts.get(status).copied().unwrap_or_default()
}

fn status_item(label: &str, value: &str, warning: bool) -> Markup {
    html! { div class="status-item" {
        span { (label) }
        span class=(if warning { "status-value warning" } else { "status-value" }) { (value) }
    } }
}

fn events_content(
    snapshot: &OperatorDashboard,
    csrf: &str,
    event: &str,
    error: Option<&str>,
) -> Markup {
    html! {
        div class="page-heading" { div { p class="eyebrow" { "Protocol input" } h1 { "Signed events" } p class="muted" { "Submit a complete signed Nostr event. The daemon applies the same deterministic validation used by the client API." } } }
        section class="panel form-panel" {
            div class="panel-header" { h2 { "Submit signed event" } span class="muted" { "JSON" } }
            form class="stack" method="post" action="/events" {
                input type="hidden" name="csrf" value=(csrf);
                @if let Some(error) = error { div class="error-box" role="alert" { strong { "Event rejected" } p { (error) } } }
                label for="event" { "Complete Nostr event" }
                textarea id="event" name="event" rows="12" spellcheck="false" placeholder=r#"{"id":"…","pubkey":"…","created_at":0,"kind":7300,"tags":[],"content":"…","sig":"…"}"# required { (event) }
                div class="form-actions" { button class="button primary" type="submit" { "Validate and submit" } }
            }
        }
        (events_panel(&snapshot.recent_events))
    }
}

fn configuration_content(
    snapshot: &OperatorDashboard,
    config: &UiRuntimeConfig,
    csrf: &str,
    relay_input: &str,
    relay_error: Option<&str>,
) -> Markup {
    let mut effective_relays = config.relays.clone();
    effective_relays.extend(snapshot.operator_relay_urls.iter().cloned());
    effective_relays.sort();
    effective_relays.dedup();
    html! {
        div class="page-heading" { div { p class="eyebrow" { "Runtime policy" } h1 { "Configuration" } p class="muted" { "Connect relays while running, review startup settings, and control economic execution." } } }
        section class="panel" {
            div class="panel-header" { h2 { "Relays" } span class="muted" { (effective_relays.len()) " configured · " a href="/configuration" { "Refresh status" } } }
            form class="stack relay-form" method="post" action="/configuration/relays" {
                input type="hidden" name="csrf" value=(csrf);
                @if let Some(error) = relay_error { div class="error-box" role="alert" { strong { "Relay not added" } p { (error) } } }
                label for="relay_url" { "Connect a relay" }
                div class="relay-form-row" {
                    input id="relay_url" name="relay_url" type="url" autocomplete="url" spellcheck="false" placeholder="wss://relay.example.com" value=(relay_input) required;
                    button class="button primary" type="submit" { "Connect relay" }
                }
                p class="muted" { "Saved relays reconnect after restart. Remote relays require secure WebSockets (wss://)." }
            }
            @if effective_relays.is_empty() { div class="empty" { "No relays yet. Add one above to start synchronization." } }
            @else { div class="table-wrap" { table {
                thead { tr { th { "Relay" } th { "Source" } th { "Status" } th { "Last connected" } th { "Error" } } }
                tbody { @for url in &effective_relays {
                    @let checkpoint = snapshot.relays.iter().find(|relay| relay.relay_url == *url);
                    tr {
                        td { code { (url) } }
                        td { @if snapshot.operator_relay_urls.contains(url) { "Operator" } @else { "Startup" } }
                        td {
                            @if let Some(row) = checkpoint {
                                @if row.last_error.is_some() { span class="badge bad" { "Needs attention" } }
                                @else if row.backfill_complete && row.last_connected_at.is_some_and(|last| u64::try_from(last).is_ok_and(|last| snapshot.generated_at.saturating_sub(last) < 15)) { span class="badge good" { "Connected" } }
                                @else { span class="badge" { "Connecting" } }
                            } @else { span class="badge" { "Connecting" } }
                        }
                        td { (optional_time(checkpoint.and_then(|row| row.last_connected_at))) }
                        td { (optional_text(checkpoint.and_then(|row| row.last_error.as_deref()))) }
                    }
                } }
            } } }
        }
        section class="panel control-panel" {
            div class="panel-header" { h2 { "Economic execution" } span class=(if snapshot.execution_enabled { "badge good" } else { "badge bad" }) { @if snapshot.execution_enabled { "Enabled" } @else { "Paused" } } }
            div class="panel-body horizontal" {
                div { p { "Connector dispatch can be paused without stopping event ingestion or projection rebuilds." } p class="muted" { "Resuming queues every stored coordination so eligible effects are reconsidered through durable idempotency checks." } }
                @if snapshot.execution_enabled {
                    (action_form("/controls/execution/pause", csrf, "Pause execution", true))
                } @else {
                    (action_form("/controls/execution/resume", csrf, "Resume execution", false))
                }
            }
        }
        section class="panel" {
            div class="panel-header" { h2 { "Effective startup configuration" } span class="badge" { "Restart managed" } }
            dl class="settings" {
                (setting("Client listener", &config.client_listen))
                (setting("Operator UI listener", &config.ui_listen))
                (setting("Relay lookback", &format!("{} seconds", config.relay_lookback_secs)))
                (setting("Work lease", &format!("{} seconds", config.work_lease_secs)))
                (setting("Connector timeout", &format!("{} ms", config.connector_timeout_ms)))
                (setting("Stream replay limit", &config.stream_max_replay.to_string()))
                (setting("Stream retention", &format!("{} seconds", config.stream_retention_secs)))
                (setting("Connector sandbox required", if config.require_connector_sandbox { "yes" } else { "no" }))
                (setting("Connector configuration", config.connector_config.as_deref().unwrap_or("not configured")))
            }
        }
        p class="muted boundary-note" { "Listeners, startup relays, connector manifests, secrets, and sandbox launchers remain restart-managed. Operator-added relays are persisted separately. Change other PONTMORE_ environment values or the connector configuration file and restart the daemon." }
    }
}

fn operations_content(snapshot: &OperatorDashboard, csrf: &str) -> Markup {
    html! {
        div class="page-heading" { div { p class="eyebrow" { "Implementation state" } h1 { "Operations" } p class="muted" { "Run bounded maintenance actions and inspect recent internal work." } } }
        section class="action-grid" {
            (action_card("Rebuild projections", "Queue reconstruction for every stored coordination.", "/controls/rebuild", "Queue rebuild", csrf, false))
            (action_card("Retry failed work", "Requeue non-expired failed work with a fresh attempt budget.", "/controls/retry-work", "Retry work", csrf, false))
            (action_card("Retry publications", "Requeue failed, already-signed outcome publications.", "/controls/retry-publications", "Retry publications", csrf, false))
            (action_card("Probe connectors", "Run an immediate health exchange with every configured connector.", "/controls/probe-connectors", "Probe now", csrf, false))
        }
        (work_panel(&snapshot.recent_work))
        (operation_panel(&snapshot.recent_operations))
        (publication_panel(&snapshot.recent_publications))
        div class="split" { (relay_panel(&snapshot.relays)) (connector_panel(&snapshot.connectors)) }
        section class="panel danger-zone" {
            div class="panel-header" { h2 { "Daemon process" } span class="badge bad" { "Danger zone" } }
            div class="panel-body horizontal" {
                div { p { "Gracefully stop client acceptance, workers, relays, connectors, and this UI." } p class="muted" { "A process supervisor must start pontmored again." } }
                form class="shutdown-form" method="post" action="/controls/shutdown" {
                    input type="hidden" name="csrf" value=(csrf);
                    label { input type="checkbox" name="confirm" value="yes" required; " I understand this stops the daemon" }
                    button class="button danger" type="submit" { "Shut down pontmored" }
                }
            }
        }
    }
}

fn action_card(
    title: &str,
    description: &str,
    action: &str,
    label: &str,
    csrf: &str,
    danger: bool,
) -> Markup {
    html! { article class="card action-card" { h2 { (title) } p class="muted" { (description) } (action_form(action, csrf, label, danger)) } }
}

fn action_form(action: &str, csrf: &str, label: &str, danger: bool) -> Markup {
    html! { form method="post" action=(action) {
        input type="hidden" name="csrf" value=(csrf);
        button class=(if danger { "button danger" } else { "button primary" }) type="submit" { (label) }
    } }
}

fn setting(label: &str, value: &str) -> Markup {
    html! { div { dt { (label) } dd { code { (value) } } } }
}

fn metric(label: &str, value: impl std::fmt::Display) -> Markup {
    html! { article class="card" { div class="card-label" { (label) } div class="card-value" { (value) } } }
}

fn coordination_panel(rows: &[OperatorCoordination]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Coordination projections" } span class="muted" { (rows.len()) " most recent" } }
        @if rows.is_empty() { div class="empty" { "No coordination roots have been projected." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Coordination" } th { "Tip" } th { "State" } th { "Applied" } th { "Rejected" } th { "Fork" } } }
            tbody { @for row in rows { tr {
                td { code title=(row.coordination_id) { (short_id(&row.coordination_id)) } }
                td { code title=(row.tip_event_id) { (short_id(&row.tip_event_id)) } }
                td { span class=(badge_class(&row.state)) { (&row.state) } }
                td { (row.applied_count) } td { (row.rejection_count) }
                td { @if row.forked { span class="badge bad" { "detected" } } @else { span class="badge good" { "none" } } }
            } } }
        } } }
    } }
}

fn relay_panel(rows: &[OperatorRelay]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Relays" } span class="muted" { (rows.len()) " configured" } }
        @if rows.is_empty() { div class="empty" { "Relay synchronization is disabled." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Relay" } th { "Backfill" } th { "Last event" } th { "Connected" } th { "Error" } } }
            tbody { @for row in rows { tr {
                td { code { (&row.relay_url) } }
                td { @if row.backfill_complete { span class="badge good" { "complete" } } @else { span class="badge" { "pending" } } }
                td { (row.last_event_created_at) } td { (optional_time(row.last_connected_at)) } td { (optional_text(row.last_error.as_deref())) }
            } } }
        } } }
    } }
}

fn connector_panel(rows: &[OperatorConnector]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Economic connectors" } span class="muted" { (rows.len()) " observed" } }
        @if rows.is_empty() { div class="empty" { "No connector health reports have been recorded." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Connector" } th { "Version" } th { "Status" } th { "Failures" } th { "Restarts" } th { "Last healthy" } th { "Error" } } }
            tbody { @for row in rows { tr {
                td { code { (&row.connector_id) } } td { (&row.package_version) }
                td { span class=(badge_class(&row.status)) { (&row.status) } }
                td { (row.consecutive_failures) } td { (row.restart_count) }
                td { (optional_time(row.last_ok_at)) } td { (optional_text(row.last_error.as_deref())) }
            } } }
        } } }
    } }
}

fn events_panel(rows: &[OperatorEvent]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Recent events" } span class="muted" { "Latest 50" } }
        @if rows.is_empty() { div class="empty" { "No protocol events have been stored." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Event" } th { "Kind" } th { "Author" } th { "Created" } th { "Received" } } }
            tbody { @for row in rows { tr {
                td { code title=(row.event_id) { (short_id(&row.event_id)) } }
                td { span class="badge" { (row.kind) } }
                td { code title=(row.pubkey) { (short_id(&row.pubkey)) } }
                td { (time(row.created_at)) }
                td { (time(row.received_at)) }
            } } }
        } } }
    } }
}

fn work_panel(rows: &[OperatorWork]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Recent work" } span class="muted" { "Latest 50" } }
        @if rows.is_empty() { div class="empty" { "No work has been queued." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Work" } th { "Type" } th { "Status" } th { "Attempts" } th { "Updated" } th { "Error" } } }
            tbody { @for row in rows { tr {
                td { code title=(row.work_id.to_string()) { (short_id(&row.work_id.to_string())) } }
                td { (&row.work_type) }
                td { span class=(badge_class(&row.status)) { (&row.status) } }
                td { (row.attempts) "/" (row.max_attempts) }
                td { (time(row.updated_at)) }
                td { (error_text(row.last_error.as_deref())) }
            } } }
        } } }
    } }
}

fn operation_panel(rows: &[OperatorOperation]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Connector operations" } span class="muted" { "Latest 50" } }
        @if rows.is_empty() { div class="empty" { "No economic connector operations have run." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Operation" } th { "Connector" } th { "Effect" } th { "Coordination" } th { "Status" } th { "Updated" } th { "Error" } } }
            tbody { @for row in rows { tr {
                td { code title=(row.operation_id.to_string()) { (short_id(&row.operation_id.to_string())) } }
                td { code { (&row.connector_id) } }
                td { (row.operation.as_deref().unwrap_or("—")) }
                td { @if let Some(id) = &row.coordination_id { code title=(id) { (short_id(id)) } } @else { span class="muted" { "—" } } }
                td { span class=(badge_class(&row.status)) { (&row.status) } }
                td { (time(row.updated_at)) }
                td { (error_text(row.error.as_deref())) }
            } } }
        } } }
    } }
}

fn publication_panel(rows: &[OperatorPublication]) -> Markup {
    html! { section class="panel" {
        div class="panel-header" { h2 { "Publication outbox" } span class="muted" { "Latest 50" } }
        @if rows.is_empty() { div class="empty" { "No signed outcomes are awaiting publication." } }
        @else { div class="table-wrap" { table {
            thead { tr { th { "Event" } th { "Coordination" } th { "Outcome" } th { "Status" } th { "Attempts" } th { "Created" } th { "Error" } } }
            tbody { @for row in rows { tr {
                td { code title=(row.event_id) { (short_id(&row.event_id)) } }
                td { code title=(row.coordination_id) { (short_id(&row.coordination_id)) } }
                td { (&row.outcome) }
                td { span class=(badge_class(&row.status)) { (&row.status) } }
                td { (row.attempts) }
                td { (time(row.created_at)) }
                td { (error_text(row.last_error.as_deref())) }
            } } }
        } } }
    } }
}

fn badge_class(status: &str) -> &'static str {
    match status {
        "healthy" | "ready" | "completed" | "succeeded" | "accepted" => "badge good",
        "failed" | "ambiguous" | "rejected" | "unhealthy" => "badge bad",
        _ => "badge",
    }
}

fn optional_time(value: Option<i64>) -> Markup {
    value.map_or_else(|| html! { span class="muted" { "never" } }, time)
}

fn time(value: i64) -> Markup {
    html! { time data-epoch=(value) { (value) } }
}

fn error_text(value: Option<&str>) -> Markup {
    value.map_or_else(
        || html! { span class="muted" { "—" } },
        |value| html! { span class="error-summary" title=(value) { (value) } },
    )
}

fn optional_text(value: Option<&str>) -> Markup {
    value.map_or_else(
        || html! { span class="muted" { "—" } },
        |value| html! { (value) },
    )
}

fn short_id(value: &str) -> String {
    if value.len() <= 16 {
        value.to_owned()
    } else {
        format!("{}…{}", &value[..8], &value[value.len() - 8..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_round_trip_and_tamper_rejection() {
        let signer = SessionSigner::new(Duration::from_secs(60), true);
        let cookie = signer.issue();
        assert!(cookie.contains("; Secure"));
        let value = cookie
            .strip_prefix(&format!("{COOKIE_NAME}="))
            .expect("cookie prefix")
            .split(';')
            .next()
            .expect("cookie value");
        assert_eq!(signer.verify(value).expect("valid session").csrf.len(), 32);
        let mut tampered = value.to_owned();
        tampered.push('0');
        assert!(signer.verify(&tampered).is_none());
    }

    #[test]
    fn markup_escapes_operator_data() {
        let connector = OperatorConnector {
            connector_id: "<script>alert(1)</script>".into(),
            package_version: "1".into(),
            status: "healthy".into(),
            consecutive_failures: 0,
            restart_count: 0,
            last_ok_at: None,
            last_error: None,
        };
        let rendered = connector_panel(&[connector]).into_string();
        assert!(!rendered.contains("<script>alert"));
        assert!(rendered.contains("&lt;script&gt;"));
    }

    #[test]
    fn origin_must_match_host() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:7301"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:7301"),
        );
        assert!(same_origin(&headers));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://example.com"),
        );
        assert!(!same_origin(&headers));
    }

    #[test]
    fn relay_urls_require_safe_websockets() {
        assert_eq!(
            normalize_relay_url(" wss://relay.example.com ").expect("secure relay"),
            "wss://relay.example.com/"
        );
        assert!(normalize_relay_url("ws://127.0.0.1:8080").is_ok());
        for input in [
            "https://relay.example.com",
            "ws://relay.example.com",
            "wss://user:password@relay.example.com",
            "wss://relay.example.com/#fragment",
            "not a URL",
        ] {
            assert!(normalize_relay_url(input).is_err(), "accepted {input}");
        }
    }
}
