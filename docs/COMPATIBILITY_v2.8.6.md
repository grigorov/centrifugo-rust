# Drop-In Compatibility: `centrifugo-rust` vs `centrifugo/centrifugo:v2.8.6`

> Live-tested against both Docker images, then re-audited after the drop-in work
> (root command = server, flag/env parity, subcommands, config mapping). All five
> surfaces are now **partial** (no "incompatible"); the remaining gaps are
> deliberately-unimplemented features and minor cosmetic/format divergences.

## 1. Verdict

**Near drop-in for the common deployment shapes; not 100% transparent.** The launch
contract now matches official: `Entrypoint` is null, `centrifugo` is on `PATH`,
`WorkingDir` is `/centrifugo`, the **bare root command is the server**, and the
default `--address` binds **all interfaces** — so `docker run IMG`,
`docker run IMG centrifugo --port 8000 --client_insecure`, `centrifugo -c config.json`,
and `centrifugo <subcommand>` all work verbatim and are reachable through published
ports. Every official flag is accepted (implemented, mapped, or accepted-with-warning
so startup never aborts), every common `CENTRIFUGO_*` env var is honored, the
maintenance subcommands (`version`/`genconfig`/`gentoken`/`checkconfig`/`checktoken`)
and HS256 tokens + standard config files are cross-compatible, and a config file's
`address`/`port`/`redis_host`/`redis_port`/`redis_url` are honored.

It **cannot** transparently substitute where you require a **feature this build does
not implement** — in-process TLS (HTTP/gRPC/Redis), the NATS broker, a separate
internal port, Redis-Sentinel-AUTH/Redis-TLS, file logging — those flags/keys are
accepted but are silent no-ops (warned at startup). Also minor: the config parser is
strictly typed (rejects viper-style quoted scalars like `"100"` / `10.0` / `"25s"`),
logs are ANSI text (not JSON), it runs **non-root on `scratch`** (no shell for
`docker exec`), and `serve` is the messaging server (Go's `serve` is a static file
server). None of these affect the wire protocol or a typical messaging deployment.

## 2. What works as drop-in

- **Launch:** `docker run IMG`; `docker run IMG centrifugo [flags]`; `centrifugo -c config.json`; `centrifugo <subcommand>`. No `--entrypoint` override needed. Default bind is all-interfaces (reachable through `-p`). `/health` and `/metrics` are always served (so `--health`/`--prometheus` are harmless no-ops).
- **Server flags:** `-a/--address`, `-p/--port`, `-c/--config`, `-e/--engine`, `-n/--name`, `--client_insecure`, `--admin`/`--admin_insecure`, `--api_insecure`, `--grpc_api`/`--grpc_api_port`, `--log_level`, `--pid_file`, and the Redis set `--redis_host`/`--redis_port`/`--redis_url`/`--redis_db`/`--redis_password`/`--redis_master_name`/`--redis_sentinels` (the Go names; we also accept a non-standard `--redis_address`). Short forms `-a/-p/-e/-n` work.
- **Env:** the full common set via `CENTRIFUGO_*` — `PORT`, `ADDRESS`, `NAME`, `ENGINE`, `CLIENT_INSECURE`, `API_KEY`, `API_INSECURE`, `ADMIN`(+`_PASSWORD`/`_SECRET`/`_INSECURE`/`_WEB_PATH`), `GRPC_API`(+`_PORT`/`_KEY`), `TOKEN_*`, `PROXY_*_ENDPOINT`, and the full `REDIS_*` set.
- **Subcommands:** `version`, `genconfig` (official key set), `gentoken` (7-day default TTL, `-t`, exp claim), `checkconfig`, `checktoken` — tokens & configs validate cross-image.
- **Config file:** standard JSON loads; `address`/`port`/`redis_host`/`redis_port`/`redis_url`, `token_hmac_secret_key`, `api_key`, `admin*`, `engine`, namespaces + channel options are honored. Unknown keys are ignored (with a warning for a curated set of inert ones).

## 3. Remaining divergences

### Unimplemented features (accepted, warned, but no-op — by design)
`--tls`/`--tls_cert`/`--tls_key`/`--tls_external` (terminate TLS at a proxy);
`--internal_address`/`--internal_port`/`--admin_external` (no public/internal split — `/metrics`,`/health`,`/admin` stay on the main port; restrict at the proxy);
`--broker`/`--nats_url` (use memory or Redis);
`--redis_tls`/`--redis_tls_skip_verify`/`--redis_sentinel_password`;
`--grpc_api_tls*`; `--debug` (no pprof); `--log_file` (logs to STDOUT). Each prints
`ignoring unsupported flag(s) …` at startup; the corresponding config-file keys print
`config keys accepted but not implemented …`.

### Minor / cosmetic
- **Config types:** strictly typed — quoted numbers (`"100"`), float-for-int (`10.0`), and Go-duration strings (`"25s"`) **fail to start** (non-zero exit). Use native JSON types.
- **Log format:** ANSI text, not official JSON — log-scraping keyed on JSON breaks.
- **Runtime:** non-root UID 10001 on `scratch` — no shell (`docker exec sh` / shell HEALTHCHECKs fail; probe `/health` externally); on native Linux, `chown` host bind-mounts to 10001 (or `--user 0`) for `--pid_file` writes.
- **`serve` collision:** Go's `serve` is a static file server; ours is the messaging server (hidden alias for root). Irrelevant to messaging deployments.
- **Per-channel-option env** (`CENTRIFUGO_PRESENCE`/`PUBLISH`/`JOIN_LEAVE`/…) is not wired — pass them as flags or in the config file.
- **Output wording:** `version` omits the `Go version:` suffix; `checktoken` omits the `payload:` line; `gentoken` prints TTL as seconds.

## 4. Migration checklist (from an official v2.8.6 deployment)

1. **Image swap, mostly as-is.** Bare `docker run IMG` and `docker run IMG centrifugo <flags>` work unchanged; default bind is all-interfaces. No `--entrypoint` change.
2. **Config value types:** unquote numbers/bools (`100` not `"100"`, `true` not `"true"`), integers not floats, no Go-duration strings — else startup fails.
3. **TLS:** terminate at a reverse proxy (`--tls*` are no-ops).
4. **Internal-port isolation:** none — restrict `/metrics`,`/health`,`/admin` at the network/proxy layer.
5. **NATS:** unsupported — use the Redis engine.
6. **Redis TLS / Sentinel-AUTH:** unsupported — use a non-TLS endpoint or a proxy.
7. **Health probes / shell tooling:** no shell on `scratch` — use an orchestrator probe against `/health`; `docker exec sh` won't work.
8. **Linux bind-mount perms:** `chown` writable mounts to UID 10001 or run `--user 0`.
9. **Logs:** ANSI text, not JSON — adjust log pipelines.
10. **Per-channel options via env:** move to flags or the config file.
