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
| Presence + join/leave | ✅ |
| Аутентификация по JWT | ✅ HMAC (HS256/384/512), RSA (RS*), ECDSA (ES256/384) |
| JWKS | ✅ выбор ключа по `kid`, фоновое обновление |
| Connect-proxy (HTTP-callback) | ✅ |
| Namespaces и приватные каналы (`$`) | ✅ |
| HTTP API (`POST /api`) | ✅ apikey-аутентификация |
| gRPC API (порт 10000) | ✅ те же 11 RPC, apikey в metadata |
| Движки | ✅ Memory (один узел) **и** Redis (мультиузловой) |
| Admin (`/admin/auth`, `/admin/api`) | ✅ аутентификация по токену |
| Метрики Prometheus (`/metrics`) | ✅ |
| Конфигурация | ✅ флаги + JSON-файл (`-c`) + env (`CENTRIFUGO_*`) |
| CLI-подкоманды | ✅ `serve`, `gentoken`, `genconfig`, `checkconfig`, `version` |

---

## Архитектура

Проект разбит на 6 крейтов (Cargo workspace):

| Крейт | Ответственность |
|---|---|
| `centrifugo-protocol` | Проводной формат: конверты Command/Reply/Push, NDJSON (inline-raw JSON), protobuf с uvarint-префиксом длины, коды ошибок (100–111), коды disconnect (3000–3013), кодек JSON/Protobuf |
| `centrifugo-auth` | Проверка JWT (HMAC/RSA/ECDSA), JWKS по `kid`, ручная проверка exp/nbf, токены подписки, генерация токенов |
| `centrifugo-core` | `Node`, шардированный `Hub`, конечный автомат `Client`, абстракция `Engine` (pub/sub + history + presence), `MemoryEngine`, connect-proxy |
| `centrifugo-grpc` | Кодогенерация tonic (server + client) из `api.proto` через чистый Rust (`protox`, без `protoc`) |
| `centrifugo-redis` | `RedisEngine`: межузловой fan-out через Redis PUB/SUB, атомарная история на Lua, presence в hash |
| `centrifugo-server` | Бинарник `centrifugo`: CLI, конфиг, HTTP (axum), WebSocket, SockJS, HTTP-/gRPC-API, admin, метрики |

### Неблокирующий fan-out (ключевое требование)

Рассылка для **10 000 / 100 000** подписчиков не блокирует друг друга:

- Каждое соединение = задача-читатель + задача-писатель, опустошающая ограниченную очередь `tokio::mpsc`.
- При публикации `Node` кодирует push **один раз на протокол**, затем `try_send` отправляет готовый кадр каждому подписчику.
- `Hub` **шардирован по хешу канала**, поэтому разные каналы рассылаются полностью параллельно; сериализуется только присвоение offset в пределах одного канала.
- Если очередь медленного подписчика переполнена — этот клиент отключается с кодом **DisconnectSlow (3008)** и удаляется, а публикующий и все остальные подписчики не затрагиваются.

### Абстракция Engine

`Engine` (async-трейт) объединяет pub/sub + history + presence. Один `Arc<dyn Engine>` стоит за `Node`:

- **MemoryEngine** — одноузловой, всё в памяти. История — кольцевой буфер по размеру + ленивый TTL; presence — карта; meta потока (offset + epoch) сохраняется и после истечения `history_lifetime` (как в Go).
- **RedisEngine** — мультиузловой. Каждый узел подписан на `centrifugo.pub.*` (PSUBSCRIBE) и маршрутизирует входящие сообщения в локальный hub. История — список + meta-hash (offset, epoch) с атомарным добавлением через Lua. Presence — hash `clientID → ClientInfo`.

---

## Сборка и запуск

Требуется Rust (stable). Для запуска оракула на Go и SDK-теста нужен Go; для Redis-тестов — `redis-server`.

```bash
# Сборка
cargo build --release          # бинарник: target/release/centrifugo

# Запуск в insecure-режиме (без токенов)
./target/release/centrifugo serve --client_insecure

# Запуск с конфиг-файлом
./target/release/centrifugo serve -c config.json

# Все тесты (юнит + конформанс)
cargo test --workspace
```

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

Основные ключи: `client_insecure`, `client_anonymous`, `token_hmac_secret_key`, `token_rsa_public_key`, `token_ecdsa_public_key`, `token_jwks_public_endpoint`, `api_key`, `api_insecure`, `engine` (`memory`|`redis`), `redis_address`, `proxy_connect_endpoint`, `grpc_api`, `grpc_api_port`, `grpc_api_key`, `admin`, `admin_password`, `admin_secret`, `channel_namespace_boundary` (`:`), `channel_private_prefix` (`$`), а также опции каналов: `presence`, `join_leave`, `presence_disable_for_client`, `history_size`, `history_lifetime`, `history_recover`, `anonymous`, `server_side`.

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

- Server-side каналы (поле `subs` в connect): требует реализации серверных подписок целиком.
- Protobuf-кодек для HTTP API (`application/octet-stream`).
- Redis Sentinel/Cluster-шардинг; presence-refresh таймер + zset TTL для очистки после падения узла; смешанный Go+Rust кластер на одном Redis (поддерживается однородный Rust-кластер).
- Admin web UI (готовый JS-бандл из дистрибутива Go — вне рамок; реализован функциональный auth и API).
- SUB_REFRESH, прокси для subscribe/publish/rpc/refresh, user-limited (`#`) каналы.

---

## Статус

Все этапы M0–M12 завершены. **133 теста проходят** (юнит + конформанс), 0 падений. Каждое проводное поведение сверено с настоящим Centrifugo v2.8.6 (golden-диффы) и подтверждено живым SDK centrifuge-go. После сквозного аудита совместимости устранены 17 расхождений с эталоном на Go.
