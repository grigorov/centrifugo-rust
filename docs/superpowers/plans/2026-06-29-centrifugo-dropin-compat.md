# Centrifugo v2.8.6 Drop-In Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `centrifugo-rust` image accept the official Centrifugo v2.8.6 launch contract, flags, env vars, subcommands, and config files so an existing official deployment can switch with minimal/no changes — without building the large absent subsystems (TLS-in-process, NATS, internal port), which are accepted-and-ignored with a warning instead of aborting.

**Architecture:** Keep the Rust wire/engine code unchanged. Restructure only the operator surface: (1) clap so the **root command runs the server** (with `serve` kept as a hidden alias so our Docker `CMD` + 200+ conformance tests keep working); (2) accept every official flag — implement the cheap/valuable ones, map the Redis ones, accept-and-ignore the unsupported ones; (3) add `CENTRIFUGO_*` env to every flag via clap's `env` feature; (4) add `checktoken`, fix `gentoken`/`genconfig`; (5) make config-file parsing lenient (type coercion + Redis key mapping). Dockerfile mirrors the official contract but **keeps non-root UID 10001 + scratch** (operator's choice).

**Tech Stack:** Rust, clap 4 (derive + **env**), axum, tracing/tracing-subscriber, serde_json, jsonwebtoken. Conformance: Rust black-box harness + Go oracle (`centrifugo/centrifugo:v2.8.6`) + Docker.

**Decisions locked:** root=server (+`serve` alias); non-root UID 10001; scratch base; unsupported flags accepted-and-ignored with a warning. Out of scope (accept-and-ignore only): in-process TLS (HTTP/gRPC/Redis), NATS broker, separate internal port, static-file `serve`, pprof `/debug`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/centrifugo-server/Cargo.toml` / root `Cargo.toml` | clap features | add `"env"` |
| `crates/centrifugo-server/src/cli.rs` | CLI shape: root args + subcommands, flag names/aliases/env, accept-and-ignore flags | major |
| `crates/centrifugo-server/src/main.rs` | dispatch (root→serve), log-level wiring, pid_file, checktoken, gentoken/genconfig | major |
| `crates/centrifugo-server/src/config.rs` | Settings fields, from_args/from_file mapping (redis_host/port/url→redis_address, log_level), lenient deser | moderate |
| `Dockerfile` | official launch contract (PATH binary, ENTRYPOINT/CMD/WORKDIR), keep USER 10001 + scratch | moderate |
| `compose.yml` | update node commands to the new contract | small |
| `conformance/tests/m26_dropin.rs` (new) | drop-in conformance: bare launch, env, redis mapping, checktoken, gentoken TTL | new |
| `docs/COMPATIBILITY_v2.8.6.md` | refresh after the audit re-run | docs |

**Note:** `/metrics` and `/health` are already routed (`http.rs:21-22`), always on — so `--prometheus`/`--health` only need to be *accepted*, not implemented.

---

## Phase 1 — Launch contract: root command runs the server

### Task 1: clap — root args + optional subcommand, `serve` kept as alias

**Files:**
- Modify: `crates/centrifugo-server/src/cli.rs`
- Modify: `crates/centrifugo-server/src/main.rs:116-135` (dispatch)

- [ ] **Step 1: Restructure `Cli` in `cli.rs`** — flatten `ServeArgs` at the root, make the subcommand optional, keep `Serve` as a hidden alias variant.

```rust
#[derive(Parser, Debug)]
#[command(name = "centrifugo", disable_version_flag = true, args_conflicts_with_subcommands = true)]
pub struct Cli {
    /// Server flags (run the server when no subcommand is given — matches Go's root command).
    #[command(flatten)]
    pub serve: ServeArgs,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Run the server (alias; the bare root command does the same).
    #[command(hide = true)]
    Serve(ServeArgs),
    /// Generate a connection JWT (HS256) for a user.
    Gentoken(GentokenArgs),
    /// Write a fresh config file with generated secrets.
    Genconfig(ConfigPathArgs),
    /// Validate a config file and exit non-zero on error.
    Checkconfig(ConfigPathArgs),
    /// Check (verify + print) a connection JWT.
    Checktoken(ChecktokenArgs),
    /// Print version and exit.
    Version,
}
```

- [ ] **Step 2: Update dispatch in `main.rs`** — run the server for `None` and for the `Serve` alias; route the rest.

```rust
let cli = Cli::parse();
match cli.command {
    None => run_server(cli.serve).await,                 // bare `centrifugo …`
    Some(Command::Serve(args)) => run_server(args).await, // `centrifugo serve …` (alias)
    Some(Command::Version) => { println!("Centrifugo v{VERSION}"); Ok(()) }
    Some(Command::Gentoken(args)) => gentoken(args),
    Some(Command::Genconfig(args)) => genconfig(&args.config),
    Some(Command::Checkconfig(args)) => checkconfig(&args.config),
    Some(Command::Checktoken(args)) => checktoken(args),  // Task 9
}
```
Refactor the existing `Command::Serve(args) => { … }` body into `async fn run_server(args: ServeArgs) -> anyhow::Result<()>`.

- [ ] **Step 3: Build + smoke-test the CLI shape**

Run:
```
export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:/opt/homebrew/bin:$PATH"
cargo build -p centrifugo-server
./target/debug/centrifugo --help            # shows server flags + subcommands
./target/debug/centrifugo version
```
Expected: `--help` lists server flags AND subcommands; `version` prints `Centrifugo v2.8.6`.

- [ ] **Step 4: Verify bare launch serves** (background, bounded)

```
./target/debug/centrifugo --client_insecure --port 18080 &  P=$!
sleep 1; curl -s -o /dev/null -w '%{http_code}\n' localhost:18080/health; kill $P
```
Expected: `200`.

- [ ] **Step 5: Run the full conformance suite** (the harness spawns `centrifugo serve …` — the alias must still work)

Run: `GOFLAGS=-mod=mod cargo test --workspace 2>&1 | grep -E "test result|FAILED"`
Expected: all pass (alias preserves harness + `serve` subcommand).

- [ ] **Step 6: Commit**

```
git add -A && git commit -m "feat(cli): root command runs the server (serve kept as hidden alias)"
```

### Task 2: Dockerfile — official launch contract (binary on PATH, ENTRYPOINT/CMD/WORKDIR), keep non-root + scratch

**Files:**
- Modify: `Dockerfile` (runtime stage)
- Modify: `compose.yml`

- [ ] **Step 1: Update the runtime stage** so `docker run img centrifugo …` works like official.

```dockerfile
# ---- runtime: scratch (nothing but the static binary) ----
FROM scratch
# Put the binary on PATH as `centrifugo` so the official launch contract works:
#   docker run IMG centrifugo -c config.json   /   docker run IMG centrifugo --client_insecure
COPY --from=builder /centrifugo /usr/local/bin/centrifugo
ENV PATH=/usr/local/bin
# Go's image reads ./config.json from the working dir; WORKDIR creates the (mountable) dir.
WORKDIR /centrifugo
# Numeric UID (no /etc/passwd on scratch); ports 8000 HTTP, 10000 gRPC.
USER 10001
EXPOSE 8000 10000
ENTRYPOINT []
# Bare `docker run IMG` starts the server (root command). Insecure default for local use;
# compose overrides `command:` for the cluster.
CMD ["centrifugo", "--client_insecure", "--address", "0.0.0.0"]
```

- [ ] **Step 2: Update `compose.yml`** — change `command:` from `["serve", …]` to the new contract.

Replace each node's `command:` so it no longer leads with `serve` (root runs the server). Example for node-1:
```yaml
    command: ["centrifugo", "--address", "0.0.0.0", "--port", "8000", "--engine", "redis", "--redis_address", "redis:6379", "--api_key", "api-secret-key", "--admin", "--admin_insecure", "--grpc_api", "--history_size", "100", "--history_lifetime", "300"]
```
(Keep flags identical to the current compose; only drop the leading `serve`.)

- [ ] **Step 3: Build the image**

Run:
```
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
docker pull rust:1-bookworm; docker pull docker/dockerfile:1   # pre-pull (flaky registry)
DOCKER_BUILDKIT=1 docker build -t centrifugo-rust:local . && echo OK
```
Expected: `OK`.

- [ ] **Step 4: Verify the official launch contract on the image**

```
docker run -d -p 18081:8000 centrifugo-rust:local centrifugo --client_insecure --address 0.0.0.0
# (uses our default CMD too: `docker run -d -p 18082:8000 centrifugo-rust:local`)
sleep 2; curl -s -o /dev/null -w '%{http_code}\n' localhost:18081/health
docker rm -f $(docker ps -lq)
```
Expected: `200`. Also test a config mount:
```
echo '{"client_insecure":true}' > /tmp/cfg.json
docker run -d -p 18083:8000 -v /tmp/cfg.json:/centrifugo/config.json centrifugo-rust:local centrifugo --address 0.0.0.0 -c /centrifugo/config.json
sleep 2; curl -s -o /dev/null -w '%{http_code}\n' localhost:18083/health; docker rm -f $(docker ps -lq)
```
Expected: `200`.

- [ ] **Step 5: Commit**

```
git add Dockerfile compose.yml && git commit -m "feat(docker): official launch contract (centrifugo on PATH, root serves); keep non-root scratch"
```

---

## Phase 2 — Accept every official flag (implement cheap ones, map Redis, ignore the rest)

### Task 3: Short aliases + string port

**Files:** Modify `crates/centrifugo-server/src/cli.rs` (`ServeArgs`)

- [ ] **Step 1: Add short forms** to existing args: `-a`/`--address`, `-p`/`--port`, `-e`/`--engine`, `-n`/`--name`. For `--port`, keep `u16` (clap parses numeric strings fine; official passes a numeric string). Example:
```rust
#[arg(short = 'a', long = "address", env = "CENTRIFUGO_ADDRESS", default_value = "127.0.0.1")]
pub address: String,
#[arg(short = 'p', long = "port", env = "CENTRIFUGO_PORT", default_value_t = 8000)]
pub port: u16,
#[arg(short = 'e', long = "engine", env = "CENTRIFUGO_ENGINE", default_value = "memory")]
pub engine: String,
#[arg(short = 'n', long = "name", env = "CENTRIFUGO_NAME", default_value = "")]
pub name: String,
```
(`env` attrs land fully in Phase 3; adding them here as we touch each arg is fine once Cargo has the feature — do Task 7 first if compiling now.)

- [ ] **Step 2: Build + verify** `./target/debug/centrifugo -p 18084 -a 0.0.0.0 --client_insecure &` → `/health` 200. Commit with Task 4 (same file).

### Task 4: Redis flag mapping + accept-and-ignore unsupported flags

**Files:** Modify `crates/centrifugo-server/src/cli.rs`, `crates/centrifugo-server/src/config.rs`, `crates/centrifugo-server/src/main.rs`

- [ ] **Step 1: Add the official Redis flags** to `ServeArgs` and map them to `redis_address`.
```rust
#[arg(long = "redis_host", env = "CENTRIFUGO_REDIS_HOST", default_value = "")]
pub redis_host: String,
#[arg(long = "redis_port", env = "CENTRIFUGO_REDIS_PORT", default_value = "")]
pub redis_port: String,
#[arg(long = "redis_url", env = "CENTRIFUGO_REDIS_URL", default_value = "")]
pub redis_url: String,
```
- [ ] **Step 2: Map in `config.rs`** — when `redis_url` set → use it as `redis_address`; else when `redis_host`/`redis_port` set → `format!("{host}:{port}")` (host default `127.0.0.1`, port `6379`); else keep `redis_address`. Add a helper `fn effective_redis_address(a:&ServeArgs)->String` and use it in both `from_args` and `from_file_and_args`.
- [ ] **Step 3: Add accept-and-ignore flags** to `ServeArgs` (boolean flags + string-valued), grouped, e.g.:
```rust
// Accepted for drop-in compatibility but NOT implemented (logged at startup).
#[arg(long = "tls")] pub tls: bool,
#[arg(long = "tls_cert", default_value = "")] pub tls_cert: String,
#[arg(long = "tls_key", default_value = "")] pub tls_key: String,
#[arg(long = "tls_external")] pub tls_external: bool,
#[arg(long = "internal_address", default_value = "")] pub internal_address: String,
#[arg(long = "internal_port", default_value = "")] pub internal_port: String,
#[arg(long = "admin_external")] pub admin_external: bool,
#[arg(long = "broker", default_value = "")] pub broker: String,
#[arg(long = "nats_url", default_value = "")] pub nats_url: String,
#[arg(long = "redis_tls")] pub redis_tls: bool,
#[arg(long = "redis_tls_skip_verify")] pub redis_tls_skip_verify: bool,
#[arg(long = "redis_sentinel_password", default_value = "")] pub redis_sentinel_password: String,
#[arg(long = "grpc_api_tls")] pub grpc_api_tls: bool,
#[arg(long = "grpc_api_tls_cert", default_value = "")] pub grpc_api_tls_cert: String,
#[arg(long = "grpc_api_tls_key", default_value = "")] pub grpc_api_tls_key: String,
#[arg(long = "grpc_api_tls_disable")] pub grpc_api_tls_disable: bool,
#[arg(long = "prometheus")] pub prometheus: bool,   // /metrics already served
#[arg(long = "health")] pub health: bool,           // /health already served
#[arg(long = "debug")] pub debug: bool,
```
- [ ] **Step 4: Warn for set-but-ignored flags** in `run_server` (main.rs) before starting: build a `Vec<&str>` of any unsupported flag that is non-default and `tracing::warn!("ignoring unsupported flag(s): {list} (no effect in this build)")`. `--prometheus`/`--health` are NOT in the list (their endpoints exist).
- [ ] **Step 5: Test** — server starts with a kitchen-sink official line:
```
./target/debug/centrifugo --client_insecure -p 18085 --tls --log_level debug --internal_port 9999 --broker nats --redis_host h --redis_port 7000 &
sleep 1; curl -s -o /dev/null -w '%{http_code}\n' localhost:18085/health; kill %1
```
Expected: `200` (warnings printed for tls/internal_port/broker; redis_host/port ignored only because engine=memory).
- [ ] **Step 6: Commit** `git commit -am "feat(cli): accept official flags — map redis_host/port/url, ignore-with-warning unsupported"`

### Task 5: `--log_level` / `--log_file`

**Files:** Modify `crates/centrifugo-server/src/cli.rs`, `crates/centrifugo-server/src/main.rs:136-141`

- [ ] **Step 1: Add flags** `--log_level` (default `info`) and `--log_file` (default ``; accept-ignore with warn — file logging not implemented).
```rust
#[arg(long = "log_level", env = "CENTRIFUGO_LOG_LEVEL", default_value = "info")] pub log_level: String,
#[arg(long = "log_file", env = "CENTRIFUGO_LOG_FILE", default_value = "")] pub log_file: String,
```
- [ ] **Step 2: Wire `log_level` into tracing** in `run_server` (replace the env-only filter):
```rust
let filter = std::env::var("RUST_LOG").ok()
    .or_else(|| if args.log_level.is_empty() { None } else { Some(map_log_level(&args.log_level)) })
    .unwrap_or_else(|| "info".into());
tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(filter)).init();
```
with `fn map_log_level(l:&str)->String` mapping Go levels → tracing: `debug→debug, info→info, error→error, fatal→error, none→off` (default `info`).
- [ ] **Step 3: Test** `./target/debug/centrifugo --client_insecure --log_level error -p 18086 &` → starts; fewer logs; `/health` 200. Kill.
- [ ] **Step 4: Commit** `git commit -am "feat(cli): --log_level drives tracing filter; --log_file accepted"`

### Task 6: `--pid_file`

**Files:** Modify `crates/centrifugo-server/src/cli.rs`, `crates/centrifugo-server/src/main.rs`

- [ ] **Step 1: Add flag** `#[arg(long="pid_file", env="CENTRIFUGO_PID_FILE", default_value="")] pub pid_file: String,`
- [ ] **Step 2: Implement** in `run_server`: if non-empty, `std::fs::write(&args.pid_file, std::process::id().to_string())?;` (best-effort: log a warning on error rather than abort, since UID 10001 may lack write perms).
- [ ] **Step 3: Test** `./target/debug/centrifugo --client_insecure --pid_file /tmp/c.pid -p 18087 & sleep 1; cat /tmp/c.pid; kill %1` → prints a PID.
- [ ] **Step 4: Commit** `git commit -am "feat(cli): --pid_file writes the process id"`

---

## Phase 3 — Environment-variable parity

### Task 7: Enable clap `env` + cover every flag

**Files:** Modify root `Cargo.toml:25`, `crates/centrifugo-server/src/cli.rs`, `crates/centrifugo-server/src/config.rs` (`apply_env`)

- [ ] **Step 1: Add the feature** — `Cargo.toml`: `clap = { version = "4", features = ["derive", "env"] }`.
- [ ] **Step 2: Add `env = "CENTRIFUGO_<UPPER>"`** to every `ServeArgs` arg not already carrying one (the names mirror official: `CENTRIFUGO_API_KEY`, `CENTRIFUGO_ADMIN`, `CENTRIFUGO_GRPC_API`, `CENTRIFUGO_TOKEN_HMAC_SECRET_KEY`, `CENTRIFUGO_REDIS_ADDRESS`, `CENTRIFUGO_REDIS_PASSWORD`, `CENTRIFUGO_REDIS_DB`, …). Booleans: clap `env` treats a present env var as the value; for parity with Go (`true`/`1`), use `#[arg(long="admin", env="CENTRIFUGO_ADMIN", action=clap::ArgAction::SetTrue, num_args=0..=0)]` — verify behavior in the test below; if clap's bool-env is awkward, parse with a small `value_parser` accepting `true|1|yes`.
- [ ] **Step 3: Retire the manual `apply_env`** in `config.rs` (clap now sources env) — or keep only the precedence rules clap can't express; delete the now-redundant `fill()` calls. Ensure `Settings::from_args` reads the clap-populated `ServeArgs` (env already applied by clap parse).
- [ ] **Step 4: Test env coverage**
```
CENTRIFUGO_PORT=18088 CENTRIFUGO_ADMIN=true CENTRIFUGO_GRPC_API=true CENTRIFUGO_CLIENT_INSECURE=true ./target/debug/centrifugo &
sleep 1
curl -s -o /dev/null -w 'health:%{http_code} admin:' localhost:18088/health
curl -s -o /dev/null -w '%{http_code}\n' localhost:18088/admin/auth -X POST
kill %1
```
Expected: `health:200 admin:200` (admin enabled via env; port from env). 
- [ ] **Step 5: Full suite** `cargo test --workspace` green (env precedence didn't break flag/file paths).
- [ ] **Step 6: Commit** `git commit -am "feat(cli): full CENTRIFUGO_* env coverage via clap env"`

---

## Phase 4 — Subcommand parity

### Task 8: `gentoken` parity (default TTL, `-t`, output format)

**Files:** Modify `crates/centrifugo-server/src/cli.rs` (`GentokenArgs`), `crates/centrifugo-server/src/main.rs:30-51`

- [ ] **Step 1: Flags** — change `ttl` to `#[arg(short='t', long="ttl", default_value_t = 604800)]` (official default 7 days + short form).
- [ ] **Step 2: Emit `exp`** when ttl>0 and a descriptive line. In `gentoken()`:
```rust
let exp = if args.ttl > 0 { Some(now + args.ttl as i64) } else { None };
// build claims { sub: user, exp? }; sign HS256
println!("HMAC SHA-256 JWT for user \"{}\" with TTL {}s:", args.user, args.ttl);
println!("{token}");
```
- [ ] **Step 3: Test** `./target/debug/centrifugo gentoken -u alice -t 60 --token_hmac_secret_key s` prints a header line + a JWT; decode to confirm `exp` present. Compare shape to `docker run --rm centrifugo/centrifugo:v2.8.6 centrifugo gentoken -u alice -t 60 -c <cfg-with-secret>`.
- [ ] **Step 4: Commit** `git commit -am "feat(gentoken): default 7-day TTL, -t short flag, exp claim + descriptive output"`

### Task 9: `checktoken` subcommand

**Files:** Modify `crates/centrifugo-server/src/cli.rs` (`ChecktokenArgs`), `crates/centrifugo-server/src/main.rs` (new `checktoken()`)

- [ ] **Step 1: Args**
```rust
#[derive(clap::Args, Debug)]
pub struct ChecktokenArgs {
    #[arg(short='c', long="config", default_value="config.json")] pub config: String,
    /// The JWT to check.
    pub token: Option<String>,
}
```
- [ ] **Step 2: Implement `checktoken()`** — read the HMAC secret from the config file (like `gentoken`), build a `TokenVerifier`, verify the token, and print the decoded user/claims; non-zero exit on invalid/missing token (mirror official messages loosely).
- [ ] **Step 3: Test** round-trip with `gentoken`:
```
T=$(./target/debug/centrifugo gentoken -u bob -t 60 --token_hmac_secret_key s | tail -1)
./target/debug/centrifugo checktoken --token_hmac_secret_key s "$T"   # prints user=bob, exit 0
./target/debug/centrifugo checktoken --token_hmac_secret_key s bogus  # exit !=0
```
- [ ] **Step 4: Commit** `git commit -am "feat(cli): add checktoken subcommand"`
(Note: `checktoken` needs the same `--token_hmac_secret_key` convenience flag as `gentoken`; add it to `ChecktokenArgs`.)

### Task 10: `genconfig` parity

**Files:** Modify `crates/centrifugo-server/src/main.rs:53-65`

- [ ] **Step 1: Emit the official key set** — `genconfig()` writes `{ "v3_use_offset": false, "token_hmac_secret_key": "<rand>", "admin_password": "<rand>", "admin_secret": "<rand>", "api_key": "<rand>", "allowed_origins": [] }` (random UUIDs as today).
- [ ] **Step 2: Test** `./target/debug/centrifugo genconfig -c /tmp/g.json && ./target/debug/centrifugo checkconfig -c /tmp/g.json` exit 0; keys present.
- [ ] **Step 3: Commit** `git commit -am "feat(genconfig): emit the official v2.8.6 starter key set"`

---

## Phase 5 — Config-file leniency

### Task 11: Redis key mapping + lenient types in `FileConfig`

**Files:** Modify `crates/centrifugo-server/src/config.rs` (`FileConfig`, `from_file_and_args`)

- [ ] **Step 1: Add `redis_host`/`redis_port`/`redis_url` to `FileConfig`** (`#[serde(default)]`) and feed them through the same `effective_redis_address` mapping (Task 4) so a config-file Redis target is honored, not silently localhost.
- [ ] **Step 2: Lenient bool/int coercion** for the shared channel-option + numeric keys — add `#[serde(deserialize_with = "de_bool_lenient")]` (accepts `true|false|"true"|"false"|0|1`) on the booleans, and a string-or-number helper on counts, so an official loosely-typed config loads. Keep `name`-required namespace behavior.
- [ ] **Step 3: Warn on accepted-but-inert keys** — after parse, if the raw JSON contains known-unsupported keys (`tls`, `log_level`, `allowed_origins`, `internal_*`, `broker`, `nats_url`, …), `tracing::warn!` listing them (parse the body into a `serde_json::Value` first to detect presence).
- [ ] **Step 4: Test** — feed an official-style config (with `redis_host`/`redis_port`, a `"presence":"true"` string, and a `tls` block) and assert: server starts, logs the redis target as the configured host (not localhost), logs an "ignored keys" warning. Add to `m26_dropin.rs`.
- [ ] **Step 5: Commit** `git commit -am "feat(config): map redis_host/port/url, lenient types, warn on inert keys"`

---

## Phase 6 — Conformance tests + re-audit + docs

### Task 12: `m26_dropin.rs` conformance tests

**Files:** Create `conformance/tests/m26_dropin.rs`

- [ ] **Step 1: Write tests** (each spawns the Rust binary via the harness; reuse `Server`/`WsJsonClient`/`api_post`). Cover: (a) bare `centrifugo --client_insecure` serves (root=server) — add a `Server::start_bare()` helper or assert via the existing spawn path with no `serve` token; (b) `CENTRIFUGO_PORT`/`CENTRIFUGO_ADMIN` env honored; (c) `--redis_host`/`--redis_port` map to the redis target (assert startup log or behavior); (d) `checktoken` round-trips a `gentoken` token; (e) `gentoken` default TTL emits `exp`; (f) an unsupported flag (`--tls`) does NOT abort startup. Concrete test bodies use the patterns already in `m6_api.rs`/`m23_server_api.rs`.
- [ ] **Step 2: Run** `cargo test --test m26_dropin` → all pass.
- [ ] **Step 3: Full suite** `cargo test --workspace` green; `cargo fmt --all --check`; `cargo clippy --all-targets -- -D warnings`.
- [ ] **Step 4: Commit** `git commit -am "test(conformance): m26 drop-in compatibility (launch/env/redis-map/checktoken/gentoken)"`

### Task 13: Rebuild image, re-run the drop-in audit, refresh the doc

- [ ] **Step 1: Rebuild** `DOCKER_BUILDKIT=1 docker build -t centrifugo-rust:local .`
- [ ] **Step 2: Re-capture** `ours_inspect.txt` / `ours_help.txt`; **re-run the audit workflow fresh** (`scriptPath` of `centrifugo-dropin-audit`, no resume).
- [ ] **Step 3: Expect** launch-runtime → compatible/partial, server-flags → partial, env-vars → compatible/partial. Update `docs/COMPATIBILITY_v2.8.6.md` with the new verdicts + a short "what changed" note.
- [ ] **Step 4: Commit** `git commit -am "docs: refresh v2.8.6 compatibility after drop-in work"`

---

## Self-Review

**Spec coverage:** Launch model → Task 1–2. Flags (aliases/redis-map/ignore/log_level/pid_file) → Task 3–6. Env → Task 7. Subcommands (gentoken/checktoken/genconfig) → Task 8–10. Config leniency → Task 11. Tests/audit/docs → Task 12–13. Out-of-scope features are explicitly accept-and-ignore (Task 4). ✓

**Ordering caveat:** clap `env` attrs (added incrementally in Phase 2) only compile once the `"env"` feature exists (Task 7 Step 1). **Mitigation:** do Task 7 Step 1 (add the feature) first, before adding any `env =` attr — or add the feature in Task 1. Adjusted: **Task 1 also adds `features=["derive","env"]`** so every later `env` attr compiles.

**Type consistency:** `run_server(args: ServeArgs)` used in Task 1/4/5/6; `effective_redis_address(&ServeArgs)` used in Task 4 + reused (file variant) in Task 11; `map_log_level(&str)` Task 5; `ChecktokenArgs`/`checktoken()` Task 9. Names consistent. ✓

**Risk:** `args_conflicts_with_subcommands=true` + flattened root args + `Serve` alias — verify `centrifugo serve --port X` (alias) AND `centrifugo --port X` (root) both parse (Task 1 Steps 3–5). If clap rejects the flatten+subcommand combo, fall back to a manual `Option<Command>` with a `Serve` default and `trailing_var_arg` — but the standard flatten pattern is expected to work.
