//! Combined local cluster member: an opaque Bitr durability service and deployment-local Foyer cache.
use std::{future::IntoFuture, sync::Arc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // What an image build checks against its label before it pushes: the
    // digest of the source tree this binary was compiled from.
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!(
            "walleye-node {} source-sha256 {}",
            env!("CARGO_PKG_VERSION"),
            walleye_node::SOURCE_SHA256
        );
        return Ok(());
    }
    // V8 sets up process-global memory protection keys, and a thread created
    // before that setup cannot later enter an isolate. Workers run on the
    // blocking pool, so the platform has to start before the runtime builds
    // any threads at all.
    walleye_v8::start();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve())
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
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
    // The embedded replica stops when this process says so, not on SIGTERM:
    // the handover below appends and flushes through its gateway.
    let (stop_bitr, bitr_stopped) = tokio::sync::oneshot::channel::<()>();
    // Without TCP_NODELAY a response split into head and body waits out the
    // peer's delayed acknowledgement, which put about 200 ms on every
    // forwarded request.
    use axum::serve::ListenerExt;
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    let server = axum::serve(listener, walleye_node::router(Arc::clone(&service)))
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .into_future();
    let bitr = async {
        if service.config.bitr {
            walleye_bitr_server::daemon::run_until(Vec::new(), async {
                let _ = bitr_stopped.await;
            })
            .await
        } else {
            std::future::pending::<Result<(), Box<dyn std::error::Error>>>().await
        }
    };
    tokio::pin!(server, bitr);
    let (result, closed) = tokio::select! {
        r=&mut server=>(r.map_err(Into::into), false),r=&mut bitr=>(r, false),r=service.discover()=>(r, false),
        _=shutdown()=> {
            // Everything from here runs with the embedded replica still
            // serving: the release flushes each table through it, and a write
            // this process accepts while it drains is made durable through it.
            let drain = async {
                // Hand the tables over first, while this process still
                // forwards requests for them to whoever claims them. The
                // server is polled throughout: its accept loop lives in this
                // future, and a connection left in the backlog while the
                // release runs is reset when the listener closes, which a
                // proxy in front reports as a 502.
                let handover = async {
                    service.release().await;
                    service.quiesce();
                    // A proxy routes to this process until it learns the
                    // Machine is stopping. Go on accepting, and forwarding
                    // to the new owners, for long enough that it has.
                    tokio::time::sleep(DRAIN_GRACE).await;
                };
                tokio::select! {
                    () = handover => {}
                    r = &mut server => {
                        service.close().await;
                        return r.map_err(Into::into);
                    }
                }
                let result: Result<(), Box<dyn std::error::Error>> = Ok(());
                let _ = stop_server.send(());
                server.await?;
                // Closing flushes too, so it happens while the replica serves.
                service.close().await;
                result
            };
            tokio::pin!(drain);
            let mut replica_running = true;
            loop {
                tokio::select! {
                    r = &mut drain => break (r, true),
                    r = &mut bitr, if replica_running => {
                        // The replica ended on its own mid-drain. The drain
                        // goes on; whatever it still has to make durable
                        // fails the way it would with the replica gone.
                        replica_running = false;
                        eprintln!("walleye.shutdown stage=drain replica=stopped outcome={}", match r {
                            Ok(()) => "ok".to_string(),
                            Err(error) => format!("error error={error}"),
                        });
                    }
                }
            }
        }
    };
    if !closed {
        service.close().await;
    }
    let _ = stop_bitr.send(());
    result
}

/// How long a stopping process keeps accepting requests after it has handed
/// its tables over. Fly Proxy was measured acting on a routing change in about
/// 0.9 to 3.5 s; a request it sends in that window is forwarded to the table's
/// new owner rather than refused.
const DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

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
