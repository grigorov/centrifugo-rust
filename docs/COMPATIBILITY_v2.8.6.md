# Drop-In Compatibility Report: `centrifugo-rust:local` vs `centrifugo/centrifugo:v2.8.6`

> Live-tested against both local Docker images (ours rebuilt from current `main`) across
> five surfaces, with an adversarial re-check (no corrections — all claims reproduced).
> Per-dimension verdicts: launch-runtime **incompatible**, subcommands **partial**,
> server-flags **incompatible**, env-vars **partial**, config-file **partial**.

## 1. Verdict

**No — `centrifugo-rust:local` is not a transparent drop-in replacement for `centrifugo/centrifugo:v2.8.6`.** An existing official deployment cannot be re-pointed at this image without changes. Three independent layers break: (a) **invocation model** — the official image runs the messaging server as the bare root command `centrifugo`, while ours runs it as the `serve` *subcommand* behind `ENTRYPOINT=/centrifugo`, so every official-style command line (`centrifugo`, `centrifugo --client_insecure`, `centrifugo -c config.json`) is rejected with a hard CLI error; (b) **flags/env coverage** — whole families of official knobs (TLS, `redis_host`/`redis_port`/`redis_url`, `log_level`, `health`, `debug`, `prometheus`, `internal_*`, `broker`/`nats`, `pid_file`, `admin_external`) are missing and hard-rejected, and only ~12 `CENTRIFUGO_*` env vars are honored; (c) **runtime shape** — non-root UID 10001, scratch (no shell), WorkingDir `/`, and `/centrifugo` is the binary file, so the canonical `-v cfg.json:/centrifugo/config.json` mount fails at container creation. **It CAN substitute** for a greenfield deployment, or one whose command/compose is rewritten to use the `serve` subcommand with long-form flags, that uses memory or Redis (via the single `--redis_address`), does not rely on TLS-at-Centrifugo / Prometheus / NATS / log-level tuning / `checktoken`, and is willing to run non-root with HTTP-based health probes. The good news: a **bare `docker run centrifugo-rust:local`** does start a working messaging server (`/health` → 200) on the same EXPOSEd ports 8000/10000, and an official `config.json` *loads* (unknown keys ignored), so the server itself is functionally compatible once you fix the launch surface.

## 2. Launch & Runtime

| Aspect | Official `v2.8.6` | `centrifugo-rust:local` | Drop-in impact |
|---|---|---|---|
| ENTRYPOINT / CMD | `Entrypoint=[]`, `Cmd=[centrifugo]` — root command **is** the server | `Entrypoint=[/centrifugo]`, `Cmd=[serve --address 0.0.0.0 --client_insecure]` — server is the `serve` subcommand | **BLOCKER**. Literal `centrifugo …` tokens become a bogus subcommand; root server flags rejected |
| User | root (UID 0) | non-root **UID 10001** | Host-volume writes owned by 10001 on Linux; default-path `genconfig`/`pid_file` fail EACCES (CWD `/` unwritable) |
| WorkingDir | `/centrifugo` (writable) | `/` (not writable by 10001) | Canonical config mount path unusable (see below) |
| Image base | Alpine — has `/bin/sh`, coreutils | **scratch** — no shell, no `id`, no coreutils | No `docker exec sh`, no shell-form HEALTHCHECK, no in-container debugging |
| Default config | auto-reads `./config.json` from `/centrifugo` | **no auto-discovery** — config silently ignored unless `-c` passed to `serve` | A mounted default `config.json` is silently not applied |
| `/centrifugo` path | a directory (config volume target) | the **binary file** | `-v cfg.json:/centrifugo/config.json` → OCI error "not a directory" at container start |
| EXPOSE | none declared | `8000/tcp`, `10000/tcp` | Cosmetic; `-p` still required, works identically |
| `version` output | `Centrifugo v2.8.6 (Go version: go1.16.6)` | `Centrifugo v2.8.6` | Same version number; Go-suffix dropped (cosmetic) |

**Exact command an operator must change:**

```bash
# OFFICIAL (works on v2.8.6, FAILS on ours):
docker run centrifugo/centrifugo:v2.8.6 centrifugo --client_insecure
docker run -v ./config.json:/centrifugo/config.json centrifugo/centrifugo:v2.8.6   # auto-reads it

# OURS (rewrite to the serve subcommand, mount elsewhere, pass -c explicitly):
docker run centrifugo-rust:local serve --address 0.0.0.0 --client_insecure
docker run -v ./config.json:/cfg.json centrifugo-rust:local serve --address 0.0.0.0 -c /cfg.json
```

The only official invocation that works unchanged is a bare `docker run <image>` (our CMD already supplies `serve …`), but note ours then runs in `--client_insecure` mode and loads no config file.

## 3. Subcommands

| Subcommand | Official | Ours | Notes |
|---|---|---|---|
| *(root)* | runs the **messaging server** with full server flags | pure subcommand dispatcher (`-h` only) | Server lives only under `serve`; no root server |
| `serve` | **static file server** on `:3000` (`-a`/`-d`/`-p`) | **the messaging server** on `:8000` | ⚠️ **NAME COLLISION — inverted meaning.** Same token, opposite service/port |
| `version` | ✅ `… (Go version: …)` | ✅ `Centrifugo v2.8.6` | Same version; Go suffix dropped |
| `genconfig` | ✅ emits `v3_use_offset`, `token_hmac_secret_key`, `admin_password`, `admin_secret`, `api_key`, `allowed_origins` | ⚠️ emits only `api_key`, `token_hmac_secret_key` | `-c` flag + default match; generated content thinner (no admin creds) |
| `checkconfig` | ✅ exit 0 valid / 1 error (silent on success) | ✅ exit 0 / 1 (prints "is valid"; different messages) | Exit-code-driven CI works unchanged |
| `gentoken` | ✅ default TTL 604800s; header+token; `-t`/`-u`/`-c` | ⚠️ default **no exp**; bare token only; **`--ttl` only (no `-t`)** | See drifts below |
| `checktoken` | ✅ `checktoken [TOKEN]` | ❌ **MISSING** — exit 2 | No workaround in the image |

**`serve` collision** — official `centrifugo serve` serves static files on `:3000`; ours `serve` is the real-time server on `:8000`. Copying either command across images silently runs the wrong service. **`checktoken` is absent** (`unrecognized subcommand`, exit 2). **`gentoken` drifts:** ours mints a *non-expiring* token by default (official mints a 7-day token), the `-t` short flag is rejected (use `--ttl`), and ours prints only the raw token (no header line) — scripts that parse by line position will misread it.

## 4. Server Flags

All server flags on ours must be passed to the **`serve` subcommand**, not the root command. **Short aliases `-a` / `-p` / `-e` / `-n` are rejected** — only `-c` is honored; use long forms. **Unknown flags are hard-rejected (server aborts)** — a single unsupported legacy flag prevents startup.

**Core / listener**

| Flag | Status | Notes |
|---|---|---|
| `--address`, `--port`, `--name`, `--engine`, `--client_insecure` | ✅ same | Long forms only; under `serve` |
| `-c` / `--config` | ✅ same | Short + long work; **no** `./config.json` auto-load |
| `--port` type | ⚠️ differs | Numeric `u16` (rejects non-numeric strings); Go accepts string |
| `-a` / `-p` / `-e` / `-n` (shorts) | ❌ missing | Rejected; use long forms |
| `--internal_address`, `--internal_port` | ❌ missing | No separate internal listener — admin/api/metrics cannot be split to a private port |
| `--pid_file` | ❌ missing | — |
| `--log_level`, `--log_file` | ❌ missing | No flag-based log control (Go default `info`/file logging) |

**API / admin**

| Flag | Status | Notes |
|---|---|---|
| `--api_insecure` | ✅ same | Ours adds `--api_key` |
| `--grpc_api`, `--grpc_api_port` | ✅ same | Default 10000; ours adds `--grpc_api_key` |
| `--admin`, `--admin_insecure` | ✅ same | Ours adds `--admin_password`/`--admin_secret`/`--admin_web_path` |
| `--admin_external` | ❌ missing | No external-port admin concept |
| `--grpc_api_tls` / `_cert` / `_key` / `_disable` | ❌ missing | No TLS for gRPC API |

**Observability / endpoints**

| Flag | Status | Notes |
|---|---|---|
| `--health` | ⚠️ differs | Ours **rejects the flag (aborts)** but serves `/health`=200 by default. Official gates `/health` behind it (404 without). Drop the flag on ours |
| `--debug` | ❌ missing | No pprof/debug endpoints |
| `--prometheus` | ❌ missing | No `/metrics` |

**TLS (HTTP server)**

| Flag | Status | Notes |
|---|---|---|
| `--tls`, `--tls_cert`, `--tls_key`, `--tls_external` | ❌ missing | Terminate TLS at a reverse proxy instead |

**Redis**

| Flag | Status | Notes |
|---|---|---|
| `--redis_host`, `--redis_port`, `--redis_url` | ❌ missing | ⚠️ **Replaced by a single `--redis_address`** (`host:port` or `redis://…`). Must rewrite |
| `--redis_address` | ➕ ours-only | The only Redis target flag ours understands |
| `--redis_password`, `--redis_db`, `--redis_master_name`, `--redis_sentinels` | ✅ same | Sentinel discovery supported |
| `--redis_sentinel_password` | ❌ missing | No Sentinel-auth flag |
| `--redis_tls`, `--redis_tls_skip_verify` | ❌ missing | TLS-Redis via flag unsupported (`rediss://` in `--redis_address` unverified) |

**Broker**

| Flag | Status | Notes |
|---|---|---|
| `--broker`, `--nats_url` | ❌ missing | No NATS broker support |

## 5. Environment Variables

Official Centrifugo uses viper with prefix `CENTRIFUGO_`, so **every** config key is env-settable. Ours hardcodes a fixed **~12-key subset** (in `config.rs::apply_env`). Names match official where present; everything else is **silently ignored** (no error, server starts on defaults).

**Honored on ours (✅, names match official):**

| Env var | Verified behavior |
|---|---|
| `CENTRIFUGO_API_KEY` | `/api` → 401 without key, 200 with `apikey` |
| `CENTRIFUGO_CLIENT_INSECURE` | Tokenless WS connect accepted (literal `"true"`) |
| `CENTRIFUGO_ENGINE` | `redis` honored (overlaid only if not set by flag/file) |
| `CENTRIFUGO_REDIS_ADDRESS` | Honored (`using redis engine at <addr>`) — *ours-only key* |
| `CENTRIFUGO_REDIS_PASSWORD`, `CENTRIFUGO_API_INSECURE`, `CENTRIFUGO_CLIENT_ANONYMOUS` | Wired via same proven `fill()` path |
| `CENTRIFUGO_TOKEN_HMAC_SECRET_KEY`, `TOKEN_RSA_PUBLIC_KEY`, `TOKEN_ECDSA_PUBLIC_KEY`, `TOKEN_JWKS_PUBLIC_ENDPOINT` | Token verification keys |
| `CENTRIFUGO_PROXY_CONNECT_ENDPOINT` | Connect-proxy endpoint |

**Ignored on ours (❌ — official honors them):**

| Env var | Impact (silent) |
|---|---|
| `CENTRIFUGO_PORT` | Ours stays on `:8000`; official moves listener. **Very common — high impact.** Use `--port` flag |
| `CENTRIFUGO_ADMIN` / `CENTRIFUGO_ADMIN_INSECURE` | Admin UI stays off (`/`, `/admin/auth` → 404). Use `--admin`/`--admin_insecure` flags |
| `CENTRIFUGO_GRPC_API` | gRPC API stays disabled. Use `--grpc_api` flag |
| `CENTRIFUGO_REDIS_HOST` / `CENTRIFUGO_REDIS_PORT` | Falls back to `127.0.0.1:6379`. **Name mismatch** — use `CENTRIFUGO_REDIS_ADDRESS=host:port` |
| `CENTRIFUGO_LOG_LEVEL` | No effect (fixed log level) |
| Every other key (`ADDRESS`, `GRPC_API_PORT`, `HISTORY_*`, `PRESENCE_*`, `PROXY_*` except connect, `ADMIN_PASSWORD/SECRET`, `NAME`, `NAMESPACES`, …) | Not read; supply via flags or `-c` JSON file instead |

**Redis env name mismatch is bidirectional:** ours reads only `CENTRIFUGO_REDIS_ADDRESS` (official ignores it → localhost default); official reads only `REDIS_HOST`/`REDIS_PORT`/`REDIS_URL` (ours ignores them → localhost default). Neither image's Redis env config ports to the other unchanged.

## 6. Config File

An official `config.json` **loads on ours without a parse error** in the common case — our `FileConfig` has no `deny_unknown_fields`, so unimplemented official keys are silently ignored, and both official `genconfig` output and a rich official-style config pass our `checkconfig` (exit 0) and boot the server.

**Honored (shared keys, 1:1 by name and type):** `token_hmac_secret_key`, `admin` / `admin_password` / `admin_secret`, `api_key` / `api_insecure`, `engine`, `client_anonymous`, `client_presence_ping_interval` / `expire_interval` (ints), `namespaces[]` (with `name` + `presence`/`join_leave`/`history_size`/`history_lifetime`/`anonymous`/`publish`), `channel_namespace_boundary`, `redis_db` / `redis_prefix` / `redis_password`.

**Three real hazards:**

1. **Silent Redis misconnect (HIGH).** Official configures Redis via `redis_host`+`redis_port` (or `redis_url`); ours has neither field. With `engine: redis`, ours boots and connects to the **default `127.0.0.1:6379`**, ignoring the operator's `redis_host`/`redis_port`/`redis_url` entirely (verified: log `using redis engine at 127.0.0.1:6379`). Rewrite to ours' single `redis_address: "host:port"`.
2. **Stricter types.** Official viper coerces loosely-typed values (e.g. `"presence": "string"`, `"history_size": "5"`) and exits 0; our serde **hard-errors and refuses to start** (`invalid type: string, expected a boolean`). A sloppily-typed-but-valid-on-official config can fail on ours. Ensure exact JSON types (bools `true/false`, counts as numbers).
3. **Accepted-but-inert keys.** TLS (`tls`/`tls_cert`/`tls_key`), `allowed_origins` (CORS), `log_level`, `history_ttl` duration-strings, `client_channel_limit`/`channel_max_length` limits, proxy timeouts, nested `proxy{}`/`tls{}`/`sockjs{}` objects — all load but **do nothing** on ours. Security/operational knobs silently have no effect; re-validate.

**Compatible edges:** unknown top-level *and* per-namespace keys are ignored (no rejection); both images require a namespace `name` (different message); both reject malformed JSON with exit 1.

## 7. Migration Checklist

1. **Rewrite the launch command** — drop the literal `centrifugo` token and prepend the `serve` subcommand. `centrifugo --client_insecure` → `serve --address 0.0.0.0 --client_insecure`. In compose, set `command: ["serve", "--address", "0.0.0.0", …]` (or rely on the default CMD for a bare insecure server).
2. **Convert short flags to long forms** — `-a/-p/-e/-n` are rejected; use `--address`/`--port`/`--engine`/`--name`. Only `-c` survives, and it must attach to `serve`.
3. **Fix the config mount** — do **not** mount to `/centrifugo/config.json` (that path is the binary). Mount to a different absolute path and pass it explicitly: `-v ./config.json:/cfg.json … serve -c /cfg.json`. There is no `./config.json` auto-discovery.
4. **Rewrite Redis target** — replace `redis_host`+`redis_port`/`redis_url` (flag, env, **and** config-file) with the single `--redis_address` / `CENTRIFUGO_REDIS_ADDRESS` / `redis_address: "host:port"`. Otherwise ours silently connects to `127.0.0.1:6379`.
5. **Convert ignored env to flags or config file** — `CENTRIFUGO_PORT`→`--port`, `CENTRIFUGO_ADMIN`/`ADMIN_INSECURE`→`--admin`/`--admin_insecure`, `CENTRIFUGO_GRPC_API`→`--grpc_api`. Most other config keys must come via the mounted `-c` JSON (which covers far more than env).
6. **Strip unsupported flags** — remove `--tls*`, `--log_level`, `--log_file`, `--health`, `--debug`, `--prometheus`, `--internal_address/port`, `--broker`, `--nats_url`, `--pid_file`, `--admin_external`, `--redis_tls*`, `--redis_sentinel_password`, `--grpc_api_tls*`. **Any one of them aborts startup.**
7. **Re-home the unsupported features:** terminate TLS at a reverse proxy; obtain metrics/pprof elsewhere (no `/metrics`, no `/debug`); accept fixed log level; switch off NATS broker (memory/Redis only). `/health` is always available — no flag needed.
8. **Adjust runtime expectations for non-root + scratch** — files written to host volumes are owned by **UID 10001** (pre-create/chown writable dirs accordingly); the default-path `genconfig`/`pid_file` writes fail (CWD `/` unwritable) so always pass an explicit writable path. Replace any `docker exec sh`/shell HEALTHCHECK with an **external HTTP probe** against `/health` (no shell in the image).
9. **Sanity-check JSON types** in the existing config (bools as `true/false`, counts as numbers) — ours rejects type mismatches that official tolerated.
10. **Replace `checktoken` usage** — that subcommand does not exist (exit 2); validate tokens with external tooling. Note `gentoken` defaults to a non-expiring token (pass `--ttl <secs>`), uses `--ttl` not `-t`, and prints only the raw token.
