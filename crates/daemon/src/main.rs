use std::process::ExitCode;

use clap::Parser;
use pontmored::{
    AppState, Config, ConnectorManager, Coordinator, DescriptorRegistry, Store, router,
};
use tokio::{net::TcpListener, sync::broadcast};
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
    let store = Store::connect(&config.database_url).await?;
    store.migrate().await?;
    let recovered = store.recover_interrupted_operations().await?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "marked interrupted connector operations ambiguous"
        );
    }
    let connectors = ConnectorManager::load(
        config.connector_config.as_deref(),
        config.connector_timeout(),
    )
    .await?;
    let descriptors = DescriptorRegistry::load(config.connector_config.as_deref()).await?;
    let (stream_sender, _) = broadcast::channel(1_024);
    let coordinator = Coordinator::new(
        store.clone(),
        connectors.clone(),
        descriptors,
        stream_sender.clone(),
    );
    coordinator.reconcile_ambiguous().await?;
    coordinator.rebuild_all().await?;
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(address = %config.listen, connectors = connectors.manifests().len(), "pontmored listening");
    axum::serve(
        listener,
        router(AppState {
            store,
            coordinator,
            stream_sender,
        }),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    connectors.shutdown().await;
    Ok(())
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
