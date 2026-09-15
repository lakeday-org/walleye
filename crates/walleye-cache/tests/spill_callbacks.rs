//! Spill creation must acquire reclaimable disk space before touching the filesystem.
use datafusion_execution::disk_manager::{DiskManager, SpillFileGuard, SpillFileObserver};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Observer(Arc<AtomicUsize>);
#[derive(Debug)]
struct Guard(Arc<AtomicUsize>);
impl SpillFileGuard for Guard {}
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl SpillFileObserver for Observer {
    fn before_create(&self) -> datafusion_common::Result<Arc<dyn SpillFileGuard>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Guard(self.0.clone())))
    }
}
#[test]
fn cloned_spill_files_keep_the_lease_until_the_file_is_deleted() {
    let live = Arc::new(AtomicUsize::new(0));
    let manager = Arc::new(
        DiskManager::builder()
            .with_spill_file_observer(Arc::new(Observer(live.clone())))
            .build()
            .expect("spill fixture"),
    );
    let file = manager.create_tmp_file("test").expect("spill fixture");
    assert_eq!(live.load(Ordering::SeqCst), 1);
    let path = file.path().to_owned();
    let other = file.clone();
    drop(file);
    assert!(path.exists());
    assert_eq!(live.load(Ordering::SeqCst), 1);
    drop(other);
    assert!(!path.exists());
    assert_eq!(live.load(Ordering::SeqCst), 0);
}
#[derive(Debug)]
struct Reject;
impl SpillFileObserver for Reject {
    fn before_create(&self) -> datafusion_common::Result<Arc<dyn SpillFileGuard>> {
        Err(datafusion_common::DataFusionError::ResourcesExhausted(
            "cache reclamation failed".into(),
        ))
    }
}
#[test]
fn failed_reclamation_prevents_file_creation() {
    let manager = Arc::new(
        DiskManager::builder()
            .with_spill_file_observer(Arc::new(Reject))
            .build()
            .expect("spill fixture"),
    );
    assert!(manager.create_tmp_file("test").is_err());
    assert_eq!(manager.spilling_progress().active_files_count, 0);
}
