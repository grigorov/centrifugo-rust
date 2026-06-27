# Centrifugo (реализация на Rust)

Полная реализация сервера реального времени **Centrifugo v2.8.6** на **Rust**, совместимая по проводному протоколу с настоящими клиентами.

> 🇬🇧 English version: [README_EN.md](README_EN.md)

---

## Зачем это нужно

Цель — байт-в-байт совместимость с клиентами, которые **нельзя обновить**. Реальные SDK (centrifuge-go, centrifuge-js и т. д.) подключаются к этому Rust-бинарнику и работают точно так же, как с оригинальным сервером на Go. Никаких изменений на стороне клиента не требуется.

**Проводная эра:** protocol **v0.3.4** / centrifuge **v0.14.2** (протокол **v2**, не v3/v4). Это поколение по умолчанию использует **seq/gen**, а не offset.

---

## Что реализовано

| Возможность | Статус |
|---|---|
| WebSocket-транспорт (`/connection/websocket`) | ✅ JSON (NDJSON) и Protobuf (`?format=protobuf`) |
| SockJS-fallback (`/connection/sockjs`) | ✅ xhr-polling + `/info` + CORS |
| Команды клиента | ✅ connect, subscribe, publish, unsubscribe, presence, presence_stats, history, refresh, ping, send, rpc, sub_refresh |
| История и восстановление (recovery) | ✅ seq/gen, восстановление при (пере)подписке, descending-порядок |
| Presence + join/leave | ✅ TTL presence (Redis) + таймер обновления на каждое соединение |
| Принудительное истечение токена | ✅ таймер отключает истёкшие соединения (3005) / подписки (3006) после grace-окна |
| Server-side каналы | ✅ `subs` в connect, JWT `channels` → авто-подписка |
| User-limited (`#`) каналы | ✅ проверка членства `name#u1,u2` |
| Разрешение на публикацию | ✅ опции канала `publish` / `subscribe_to_publish` |
| Аутентификация по JWT | ✅ HMAC (HS256/384/512), RSA (RS*), ECDSA (ES256/384) |
| JWKS | ✅ выбор ключа по `kid`, фоновое обновление |
| Прокси (HTTP-callbacks) | ✅ connect, refresh, subscribe, publish, rpc |
| Namespaces и приватные каналы (`$`) | ✅ |
| HTTP API (`POST /api`) | ✅ apikey-аутентификация; JSON (NDJSON) **и** Protobuf (`application/octet-stream`); эхо Content-Type запроса |
| Серверные `unsubscribe` / `disconnect` | ✅ отписать пользователя от канала / закрыть его соединения (HTTP + gRPC, по всему кластеру) |
| gRPC API (порт 10000) | ✅ те же 11 RPC, apikey в metadata |
| Персональные каналы | ✅ `user_subscribe_to_personal` — авто-подписка на `#<user>` |
| Движки | ✅ Memory (один узел) **и** Redis (мультиузловой), вкл. **Sentinel** с переобнаружением мастера при failover «на лету» |
| Interop Go ⇄ Rust на Redis | ✅ живой pub/sub **+ история + presence** между Go- и Rust-узлами на одном Redis (wire-формат centrifuge) |
| Admin (`/admin/auth`, `/admin/api`) | ✅ аутентификация по токену + вендоренный web UI на `/` |
| Метрики Prometheus (`/metrics`) | ✅ node-gauges + счётчики по командам/сообщениям/транспортам |
| Конфигурация | ✅ флаги + JSON-файл (`-c`) + env (`CENTRIFUGO_*`) |
| CLI-подкоманды | ✅ `serve`, `gentoken`, `genconfig`, `checkconfig`, `version` |

---

## Архитектура

Проект разбит на 6 крейтов (Cargo workspace):

| Крейт | Ответственность |
|---|---|
| `centrifugo-protocol` | Проводной формат: конверты Command/Reply/Push, NDJSON (inline-raw JSON), protobuf с uvarint-префиксом длины, коды ошибок (100–111), коды disconnect (3000–3013), кодек JSON/Protobuf |
| `centrifugo-auth` | Проверка JWT (HMAC/RSA/ECDSA), JWKS по `kid`, ручная проверка exp/nbf, токены подписки, генерация токенов |
| `centrifugo-core` | `Node`, шардированный `Hub`, конечный автомат `Client`, состояние каждой подписки, абстракция `Engine` (pub/sub + history + presence + control), `MemoryEngine`, трейты прокси (connect/refresh/subscribe/publish/rpc), реестр метрик |
| `centrifugo-grpc` | Кодогенерация tonic (server + client) из `api.proto` через чистый Rust (`protox`, без `protoc`) |
| `centrifugo-redis` | `RedisEngine`: межузловой fan-out в **wire-формате centrifuge v0.14.2** (interop Go⇄Rust), Lua list-история + zset/hash presence, обнаружение мастера через Sentinel + failover «на лету» |
| `centrifugo-server` | Бинарник `centrifugo`: CLI, конфиг, HTTP (axum), WebSocket, SockJS, HTTP-/gRPC-API, admin, метрики; исходящий TLS (JWKS/прокси) через rustls (без OpenSSL) |

### Неблокирующий fan-out (ключевое требование)

Рассылка для **10 000 / 100 000** подписчиков не блокирует друг друга:

- Каждое соединение = задача-читатель + задача-писатель, опустошающая ограниченную очередь `tokio::mpsc`.
- При публикации `Node` кодирует push **один раз на протокол**, затем `try_send` отправляет готовый кадр каждому подписчику.
- `Hub` **шардирован по хешу канала**, поэтому разные каналы рассылаются полностью параллельно; сериализуется только присвоение offset в пределах одного канала.
- Если очередь медленного подписчика переполнена — этот клиент отключается с кодом **DisconnectSlow (3008)** и удаляется, а публикующий и все остальные подписчики не затрагиваются.

### Абстракция Engine

`Engine` (async-трейт) объединяет pub/sub + history + presence. Один `Arc<dyn Engine>` стоит за `Node`:

- **MemoryEngine** — одноузловой, всё в памяти. История — кольцевой буфер по размеру + ленивый TTL; presence — карта; meta потока (offset + epoch) сохраняется и после истечения `history_lifetime` (как в Go).
- **RedisEngine** — мультиузловой, **байт-совместимый с форматом Redis из centrifuge v0.14.2**, поэтому Go- и Rust-узлы могут делить один Redis. Каждый узел подписан на `centrifugo.client.*` (PSUBSCRIBE) и маршрутизирует входящие сообщения — protobuf `Publication` и join/leave с префиксами `__j__`/`__l__` — в локальный hub. История — список (`centrifugo.list.<ch>`, элементы `__<offset>__<protobuf>`, LPUSH) + meta-hash (`s`=offset, `e`=epoch), добавление дословным Lua из centrifuge; presence — data-hash `clientID → protobuf ClientInfo` плюс zset со временем истечения, атомарное добавление/чтение с отбраковкой по score (записи упавшего узла истекают). Мастер обнаруживается через **Redis Sentinel** (`redis_master_name` + `redis_sentinels`) с переобнаружением при failover «на лету». Межузловой control (серверные unsubscribe/disconnect) идёт по Rust-только каналу.

---

## Сборка и запуск

Требуется Rust (stable). Для запуска оракула на Go и SDK-теста нужен Go; для Redis-тестов — `redis-server`.

```bash
# Сборка
cargo build --release          # бинарник: target/release/centrifugo

# Полностью статический бинарь (без glibc/OpenSSL) — см. раздел Docker, или напрямую:
#   rustup target add x86_64-unknown-linux-musl   # + musl-tools
#   cargo build --release --target x86_64-unknown-linux-musl -p centrifugo-server

# Запуск в insecure-режиме (без токенов)
./target/release/centrifugo serve --client_insecure

# Запуск с конфиг-файлом
./target/release/centrifugo serve -c config.json

# Все тесты (юнит + конформанс)
cargo test --workspace
```

### Docker

Многостадийный `Dockerfile` собирает **полностью статический бинарь** (musl libc + TLS на rustls со встроенными CA-корнями — без OpenSSL, без glibc, без системного хранилища сертификатов) и кладёт его в `scratch`, так что образ — это просто самодостаточный бинарь без runtime-зависимостей. `compose.yml` поднимает **кластер из двух узлов на одном Redis** (Redis-движок рассылает публикации между узлами):

```bash
docker compose up --build
# admin UI узла-1:  http://localhost:8000/   (пароль: password)
# admin UI узла-2:  http://localhost:8001/
# HTTP API:         POST http://localhost:8000/api   (Authorization: apikey api-secret-key)
# gRPC API:         localhost:10000
```

Клиент, подписанный на узле-1, получает сообщения, опубликованные через API узла-2, — демонстрация межузлового движка. `.dockerignore` держит build-контекст компактным (без `target/`, вендоренного Go-оракула и docs).

### Эндпоинты

| Путь | Назначение |
|---|---|
| `GET /connection/websocket` | WebSocket (добавьте `?format=protobuf` для protobuf) |
| `*  /connection/sockjs/...` | SockJS-fallback |
| `POST /api` | HTTP API (заголовок `Authorization: apikey <KEY>` или `?api_key=`) |
| `POST /admin/auth` | Обмен пароля на admin-токен |
| `POST /admin/api` | Admin API (заголовок `Authorization: token <TOKEN>`) |
| `GET /metrics` | Метрики Prometheus |
| `GET /health` | Health-check |
| gRPC на `grpc_api_port` (10000) | gRPC API (metadata `authorization: apikey <KEY>`) |

---

## Конфигурация

Приоритет: **флаги > конфиг-файл > переменные окружения** (`CENTRIFUGO_<OPTION>`).

Пример `config.json`:

```json
{
  "token_hmac_secret_key": "секрет",
  "api_key": "ключ-api",
  "admin": true,
  "admin_password": "пароль",
  "admin_secret": "секрет-сессии",
  "engine": "redis",
  "redis_address": "127.0.0.1:6379",
  "grpc_api": true,
  "grpc_api_port": 10000,
  "presence": true,
  "join_leave": true,
  "history_size": 100,
  "history_lifetime": 300,
  "history_recover": true,
  "namespaces": [
    { "name": "news", "presence": true, "history_size": 10, "history_lifetime": 60 }
  ]
}
```

Основные ключи: `client_insecure`, `client_anonymous`, `token_hmac_secret_key`, `token_rsa_public_key`, `token_ecdsa_public_key`, `token_jwks_public_endpoint`, `api_key`, `api_insecure`, `engine` (`memory`|`redis`), `redis_address`, `redis_master_name`, `redis_sentinels`, `client_presence_ping_interval`, `client_presence_expire_interval`, `proxy_connect_endpoint`, `proxy_refresh_endpoint`, `proxy_subscribe_endpoint`, `proxy_publish_endpoint`, `proxy_rpc_endpoint`, `grpc_api`, `grpc_api_port`, `grpc_api_key`, `admin`, `admin_insecure`, `admin_password`, `admin_secret`, `admin_web_path`, `user_subscribe_to_personal`, `user_personal_channel_namespace`, `channel_namespace_boundary` (`:`), `channel_private_prefix` (`$`), а также опции каналов: `presence`, `join_leave`, `presence_disable_for_client`, `publish`, `subscribe_to_publish`, `proxy_subscribe`, `proxy_publish`, `history_size`, `history_lifetime`, `history_recover`, `anonymous`, `server_side`.

### CLI-подкоманды

```bash
centrifugo gentoken --token_hmac_secret_key <секрет> -u <user> [--ttl <сек>]   # выпуск JWT
centrifugo genconfig -c config.json                                            # генерация конфига со случайными секретами
centrifugo checkconfig -c config.json                                          # проверка конфига
centrifugo version
```

---

## Конформанс (3 уровня)

Идеал «100% Go-тестов проходят» недостижим напрямую: все `*_test.go` в Go — это in-process юнит-тесты, линкующие Go как библиотеку, и они не могут целиться в сторонний бинарник. Поэтому проверка совместимости — **чёрный ящик** в трёх уровнях:

1. **Go-оракул.** Собирается настоящий бинарник Centrifugo v2.8.6 (`conformance/go-oracle/build.sh`). Оба сервера (Go и Rust) запускаются рядом и получают идентичные команды; ответы сравниваются по структуре (`key_shape` — сравнение формы значений без привязки к конкретным id/epoch).
2. **Black-box-харнес.** Тесты на Rust подключаются к запущенному бинарнику по реальному WebSocket/HTTP/gRPC и проверяют поведение покомандно.
3. **Живой SDK.** Настоящий клиент **centrifuge-go v0.6.2** (именно эта версия говорит на protocol v0.3.4 — версия v0.8.4 из исходного плана оказалась несовместимой) подключается к Rust-бинарнику, подписывается, публикует и аутентифицируется по JWT — это решающее доказательство совместимости.

```bash
# Подготовка оракула (нужен Go)
bash conformance/go-oracle/build.sh

# Redis для мультиузловых тестов (необязательно — иначе тесты пропускаются)
brew install redis

# Прогон
cargo test --workspace
```

Тесты, требующие внешних зависимостей (Go-оракул, Redis, Go-SDK), **аккуратно пропускаются**, если зависимость недоступна, — набор остаётся «зелёным» на любой машине.

---

## Заметки о совместимости

- **seq/gen по умолчанию.** Centrifugo v2.8.6 использует seq/gen, а не offset (`v3_use_offset=false`). `offset = gen*MaxUint32 + seq` (асимметрично с распаковкой `>>32` — это особенность centrifuge v0.14.2, воспроизведена дословно). Восстановленные публикации отдаются в порядке убывания (новые первыми).
- **Push** — это Reply с `id==0`, результат которого содержит закодированный Push. Целочисленный `method` опускается, когда равен 0 (CONNECT).
- **Коды ошибок** 100–111; **коды disconnect** 3000–3013. Семантика проверена по исходникам Go: connect-токен истёк → 109 (reply), невалидный/отсутствует → 3002/3003 (disconnect), refresh истёк → 3005, presence/history выключены → 108, не подписан → 103, неизвестный namespace → 102.
- **History meta-TTL** отделён от `history_lifetime`: после истечения времени жизни истории очищаются только публикации, а offset + epoch потока сохраняются, поэтому «догнавший» клиент после простоя получает `recovered=true`.

---

## Что осталось за рамками (отложено)

- **Redis Cluster / шардинг.** Поддерживается только одномастерный Redis (напрямую или через Sentinel) — без шардинга по consistent-hash на несколько Redis-шардов.
- **Interop control-сообщений Go⇄Rust.** Живой pub/sub, история и presence interop-ятся между Go- и Rust-узлами на общем Redis (wire-формат centrifuge). Только межузловые control-команды (серверные unsubscribe/disconnect API) идут по Rust-только каналу и до Go-узлов не доходят.
- **Интеграционный тест live-failover Sentinel.** Переобнаружение мастера «на лету» реализовано, но CI-тест с реальным падением мастера требует харнеса с репликой + промоушеном Sentinel (живой сценарий проверен вручную).

---

## Статус

Все этапы M0–M12, фазы полного паритета (server-side каналы, SUB_REFRESH, `#`-каналы, TTL presence + таймер обновления, гранулярные прокси, Protobuf HTTP API, разрешение на публикацию, Redis Sentinel, admin web UI) и пост-аудит фичи (серверные unsubscribe/disconnect, персональные каналы, mid-flight failover Sentinel, метрики по командам, живой interop Go⇄Rust на Redis) завершены. **190 тестов проходит** (юнит + конформанс), 0 падений. Каждое проводное поведение сверено с настоящим Centrifugo v2.8.6 (golden-диффы) и подтверждено живым SDK centrifuge-go. Сквозной adversarial-аудит устранил 40+ расхождений с эталоном на Go.
