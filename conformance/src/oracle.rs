//! Spawns the real Go centrifugo v2.8.6 (built by `go-oracle/build.sh`) as a
//! differential behavior oracle. Returns `None` when the binary is absent so the
//! suite stays green on machines without Go / without the oracle built.

use std::process::{Child, Command};
use std::time::Duration;

use crate::pick_port;

pub struct Oracle {
    child: Child,
    pub port: u16,
    pub http: String,
}

fn oracle_bin() -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("go-oracle");
    p.push("bin");
    p.push("centrifugo");
    p
}

impl Oracle {
    /// Start the Go oracle in insecure client mode. `None` if the binary is
    /// missing or never becomes healthy (logged, so the calling test can skip).
    pub async fn start() -> Option<Oracle> {
        let bin = oracle_bin();
        if !bin.exists() {
            eprintln!(
                "go oracle binary absent ({}); skipping differential test (run conformance/go-oracle/build.sh)",
                bin.display()
            );
            return None;
        }
        let port = pick_port();
        let child = Command::new(&bin)
            .args([
                "--client_insecure",
                "--health",
                "--port",
                &port.to_string(),
                "--log_level",
                "error",
            ])
            .spawn()
            .ok()?;
        let mut oracle = Oracle {
            child,
            port,
            http: format!("http://127.0.0.1:{port}"),
        };
        let client = reqwest::Client::new();
        for _ in 0..100 {
            if let Ok(resp) = client.get(format!("{}/health", oracle.http)).send().await {
                if resp.status().is_success() {
                    return Some(oracle);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        eprintln!("go oracle did not become healthy on port {port}; skipping differential test");
        let _ = oracle.child.kill();
        None
    }

    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}/connection/websocket", self.port)
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
