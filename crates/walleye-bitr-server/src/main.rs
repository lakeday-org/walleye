//! Standalone Bitr durability process.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    walleye_bitr_server::daemon::run_from_env().await
}
