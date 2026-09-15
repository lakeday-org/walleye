//! A gateway that accepts a connection and never answers must fail the call.
//!
//! The Lance host calls the gateway from the isolate thread, blocking it
//! until the response arrives. Without a request timeout a stalled gateway
//! holds that thread forever, and the V8 execution deadline cannot help: it
//! only interrupts JavaScript, never a host call in flight.

use std::time::{Duration, Instant};

use walleye_bitr::{HttpReplica, ReplicaError, ReplicaGateway};

/// Accepts every connection and holds it open without ever writing a byte.
async fn silent_gateway() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    address
}

#[tokio::test]
async fn a_silent_gateway_fails_the_call_instead_of_hanging() {
    let address = silent_gateway().await;
    let gateway = HttpReplica::with_timeouts(
        format!("http://{address}"),
        "gateway-secret",
        Duration::from_millis(500),
        Duration::from_millis(500),
    );
    let started = Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(3), gateway.recover("tenant-a/do-7", 0)).await;
    let elapsed = started.elapsed();
    let result = outcome.unwrap_or_else(|_| {
        panic!("recover was still waiting on a silent gateway after {elapsed:?}")
    });
    assert!(
        matches!(result, Err(ReplicaError::GatewayUnavailable)),
        "a stalled gateway must surface as unavailable, got {result:?}"
    );
}
