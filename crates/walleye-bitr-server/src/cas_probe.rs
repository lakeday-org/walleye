//! Tests the configured provider's conditional writes without touching the
//! live control head or any stream. The probe removes its isolated object.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct CasProbeResult {
    pub conditional_create: bool,
    pub conditional_update: bool,
    pub stale_update_rejected: bool,
    pub cleanup: bool,
}

type ProbeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn conflict(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    )
}

async fn put(
    store: &dyn ObjectStore,
    path: &Path,
    value: &'static [u8],
    mode: PutMode,
) -> object_store::Result<object_store::PutResult> {
    store
        .put_opts(
            path,
            Bytes::from_static(value).into(),
            PutOptions {
                mode,
                ..PutOptions::default()
            },
        )
        .await
}

fn one_winner(
    left: object_store::Result<object_store::PutResult>,
    right: object_store::Result<object_store::PutResult>,
) -> ProbeResult<bool> {
    match (left, right) {
        (Ok(_), Err(error)) if conflict(&error) => Ok(true),
        (Err(error), Ok(_)) if conflict(&error) => Ok(false),
        (Err(error), _) | (_, Err(error)) => Err(error.into()),
        (Ok(_), Ok(_)) => Err("provider accepted two conflicting conditional writes".into()),
    }
}

pub async fn verify(store: Arc<dyn ObjectStore>, prefix: &str) -> ProbeResult<CasProbeResult> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = Path::from(format!(
        "{}/control/cas-probe-{nonce}-{}/head",
        prefix.trim_matches('/'),
        std::process::id()
    ));
    let result = async {
        let (left, right) = tokio::join!(
            put(store.as_ref(), &path, b"create-left", PutMode::Create),
            put(store.as_ref(), &path, b"create-right", PutMode::Create),
        );
        let left_won = one_winner(left, right)?;
        let current = store.get(&path).await?;
        let version = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        if version.e_tag.is_none() {
            return Err("provider omitted the ETag required for metadata CAS".into());
        }
        let expected: &[u8] = if left_won {
            b"create-left"
        } else {
            b"create-right"
        };
        if current.bytes().await?.as_ref() != expected {
            return Err("conditional create winner does not match readback".into());
        }
        let (left, right) = tokio::join!(
            put(
                store.as_ref(),
                &path,
                b"update-left",
                PutMode::Update(version.clone())
            ),
            put(
                store.as_ref(),
                &path,
                b"update-right",
                PutMode::Update(version.clone())
            ),
        );
        let left_won = one_winner(left, right)?;
        match put(store.as_ref(), &path, b"stale", PutMode::Update(version)).await {
            Err(error) if conflict(&error) => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("provider accepted a stale metadata ETag".into()),
        }
        let expected: &[u8] = if left_won {
            b"update-left"
        } else {
            b"update-right"
        };
        if store.get(&path).await?.bytes().await?.as_ref() != expected {
            return Err("conditional update winner does not match readback".into());
        }
        Ok(CasProbeResult {
            conditional_create: true,
            conditional_update: true,
            stale_update_rejected: true,
            cleanup: true,
        })
    }
    .await;
    let cleanup = store.delete(&path).await;
    cleanup?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn probe_checks_conditional_writes_and_cleans_its_object() -> ProbeResult<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let live_head = Path::from("test/control/head.json");
        put(
            store.as_ref(),
            &live_head,
            b"live-head-untouched",
            PutMode::Create,
        )
        .await?;
        let result = verify(Arc::clone(&store), "test").await?;
        assert!(
            result.conditional_create
                && result.conditional_update
                && result.stale_update_rejected
                && result.cleanup
        );
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location, live_head);
        assert_eq!(
            store.get(&live_head).await?.bytes().await?.as_ref(),
            b"live-head-untouched"
        );
        Ok(())
    }
}
