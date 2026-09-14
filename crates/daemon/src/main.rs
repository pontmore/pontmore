use std::process::ExitCode;

use clap::Parser;
use pontmored::{
    AppState, Config, ConnectorManager, Coordinator, DescriptorRegistry, Store,
    relay::RelayService, router,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    time::Duration,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match run(Config::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "pontmored stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    if Duration::from_secs(config.work_lease_secs) <= config.connector_timeout() {
        return Err("PONTMORE_WORK_LEASE_SECS must exceed PONTMORE_CONNECTOR_TIMEOUT_MS".into());
    }
    let store = Store::connect(&config.database_url).await?;
    store.migrate().await?;
    store.activate_execution().await?;
    let recovered = store.recover_interrupted_operations().await?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "marked interrupted connector operations ambiguous"
        );
    }
    let connectors = ConnectorManager::load_with_policy(
        config.connector_config.as_deref(),
        config.connector_timeout(),
        config.require_connector_sandbox,
    )
    .await?;
    let descriptors = DescriptorRegistry::load_with_store(
        config.connector_config.as_deref(),
        store.clone(),
        pontmored::schema::SchemaFetcher::new(config.connector_timeout(), config.schema_max_bytes),
    )
    .await?;
    let (stream_sender, _) = broadcast::channel(1_024);
    let coordinator = Coordinator::new(
        store.clone(),
        connectors.clone(),
        descriptors,
        stream_sender.clone(),
    );
    coordinator.reconcile_ambiguous().await?;
    coordinator.enqueue_all().await?;
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let worker = tokio::spawn({
        let coordinator = coordinator.clone();
        let shutdown = shutdown_receiver.clone();
        async move {
            coordinator
                .run_worker(Duration::from_secs(config.work_lease_secs), shutdown)
                .await;
        }
    });
    let relay = RelayService::new(
        config.relays.clone(),
        store.clone(),
        coordinator.clone(),
        stream_sender.clone(),
        Duration::from_secs(config.relay_lookback_secs),
    );
    let relay_task = tokio::spawn(async move {
        if let Err(error) = relay.run(shutdown_receiver).await {
            tracing::error!(%error, "relay service stopped");
        }
    });
    let retention_task = spawn_retention(
        store.clone(),
        Duration::from_secs(config.stream_retention_secs),
        shutdown_sender.subscribe(),
    );
    let health_task = spawn_health(
        store.clone(),
        connectors.clone(),
        shutdown_sender.subscribe(),
    );
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(address = %config.listen, connectors = connectors.manifests().len(), "pontmored listening");
    axum::serve(
        listener,
        router(AppState {
            store,
            coordinator,
            stream_sender,
            max_replay: config.stream_max_replay,
            required_relays: config.relays.len(),
        }),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    let _ = shutdown_sender.send(true);
    let _ = worker.await;
    let _ = relay_task.await;
    let _ = retention_task.await;
    let _ = health_task.await;
    connectors.shutdown().await;
    Ok(())
}

fn spawn_retention(
    store: Store,
    retention: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    if let Err(error) = store.prune_stream(retention).await { tracing::error!(%error, "stream retention failed"); }
                }
                _ = shutdown.changed() => break,
            }
        }
    })
}

fn spawn_health(
    store: Store,
    connectors: ConnectorManager,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            for health in connectors.health_all().await {
                if let Err(error) = store.record_connector_health(&health).await {
                    tracing::error!(%error, "connector health persistence failed");
                }
            }
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(15)) => {},
                _ = shutdown.changed() => break,
            }
        }
    })
}

async fn shutdown_signal() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
    tracing::info!("shutdown requested");
}
