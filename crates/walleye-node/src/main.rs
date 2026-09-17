//! Combined local cluster member: an opaque Bitr durability service and deployment-local Foyer cache.
use std::{future::IntoFuture, sync::Arc};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = if std::env::var_os("WALLEYE_DISCOVERY_SERVICE").is_some() {
        walleye_node::kubernetes::config_from_env()?
    } else if let Some(path) = std::env::var("WALLEYE_CONFIG").ok().or_else(|| {
        std::fs::exists("/etc/walleye/config.json")
            .ok()?
            .then(|| "/etc/walleye/config.json".into())
    }) {
        serde_json::from_slice(&std::fs::read(path)?)?
    } else {
        walleye_node::Config::from_env()?
    };
    let service = walleye_node::Service::open(config).await?;
    let listener = match tokio::net::TcpListener::bind(&service.config.listen).await {
        Ok(listener) => listener,
        // A host without IPv6 cannot bind the dual-stack default; fall back to
        // IPv4 on the same port rather than refusing to start.
        Err(error) if service.config.listen.starts_with("[::]:") => {
            let v4 = format!("0.0.0.0:{}", &service.config.listen["[::]:".len()..]);
            eprintln!(
                "walleye.listen bind={} outcome=fallback to={v4} error={error}",
                service.config.listen
            );
            tokio::net::TcpListener::bind(&v4).await?
        }
        Err(error) => return Err(error.into()),
    };
    let (stop_server, stopped) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, walleye_node::router(Arc::clone(&service)))
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .into_future();
    let bitr = async {
        if service.config.bitr {
            walleye_bitr_server::daemon::run_with_arguments(Vec::new()).await
        } else {
            std::future::pending::<Result<(), Box<dyn std::error::Error>>>().await
        }
    };
    let processor = async {
        match service.config.processor.clone() {
            Some(config) => service
                .process(config)
                .await
                .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() }),
            None => std::future::pending().await,
        }
    };
    tokio::pin!(server, processor);
    let result = tokio::select! {
        r=&mut server=>r.map_err(Into::into),r=bitr=>r,r=service.discover()=>r,r=&mut processor=>r,
        _=shutdown()=> {
            service.quiesce();
            // Keep serving state commits until the outstanding HTTP processors return.
            let result = if service.config.processor.is_some() {
                tokio::select! {r=&mut processor=>r, r=&mut server=>{service.close().await; return r.map_err(Into::into);}}
            } else { Ok(()) };
            let _ = stop_server.send(());
            server.await?;
            result
        }
    };
    service.close().await;
    result
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
