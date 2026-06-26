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
        Oracle::start_with(&["--client_insecure"]).await
    }

    /// Start the Go oracle with explicit extra flags (besides `--health`,
    /// `--port`, `--log_level error`). `None` if the binary is absent/unhealthy.
    pub async fn start_with(extra_args: &[&str]) -> Option<Oracle> {
        Oracle::start_with_env(extra_args, &[]).await
    }

    /// Like `start_with`, plus `CENTRIFUGO_*`-style env vars.
    pub async fn start_with_env(extra_args: &[&str], env: &[(&str, &str)]) -> Option<Oracle> {
        let bin = oracle_bin();
        if !oracle_present(&bin) {
            return None;
        }
        let port = pick_port();
        let mut cmd = Command::new(&bin);
        cmd.args(base_args(port));
        cmd.args(extra_args);
        for (k, v) in env {
            cmd.env(k, v);
        }
        spawn_and_wait(cmd, port).await
    }

    /// Start the Go oracle with a JSON config file (the reliable way to set
    /// options Go exposes only via config, e.g. history_size). The config is
    /// written to a temp file and passed via `-c`.
    pub async fn start_with_config(config_json: &str) -> Option<Oracle> {
        let bin = oracle_bin();
        if !oracle_present(&bin) {
            return None;
        }
        let port = pick_port();
        let cfg_path = std::env::temp_dir().join(format!("centrifugo-oracle-{port}.json"));
        if std::fs::write(&cfg_path, config_json).is_err() {
            return None;
        }
        let mut cmd = Command::new(&bin);
        cmd.arg("-c").arg(&cfg_path);
        cmd.args(base_args(port));
        spawn_and_wait(cmd, port).await
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

fn oracle_present(bin: &std::path::Path) -> bool {
    if bin.exists() {
        true
    } else {
        eprintln!(
            "go oracle binary absent ({}); skipping differential test (run conformance/go-oracle/build.sh)",
            bin.display()
        );
        false
    }
}

fn base_args(port: u16) -> Vec<String> {
    vec![
        "--health".into(),
        "--port".into(),
        port.to_string(),
        "--log_level".into(),
        "error".into(),
    ]
}

async fn spawn_and_wait(mut cmd: Command, port: u16) -> Option<Oracle> {
    let child = cmd.spawn().ok()?;
    let mut oracle = Oracle {
        child,
        port,
        http: format!("http://127.0.0.1:{port}"),
    };
    let client = reqwest::Client::new();
    for _ in 0..200 {
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
