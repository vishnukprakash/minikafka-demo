# Live Order Board — a demo app for minikafka

A producer service publishes simulated food orders to a [minikafka](https://github.com/vishnukprakash/minikafka)
cluster, a consumer service reads them as consumer group `order-board`, and one web page shows
orders being **sent** and **received** in real time, with the consumer's **offset**, **lag**, and
**resume-from-committed-offset** behaviour visible.

```
┌────────────────┐   produce    ┌──────────────────────────┐   consume   ┌────────────────┐
│ order-producer │ ───────────▶ │ minikafka cluster        │ ──────────▶ │ order-consumer │
│  (:8081)       │  acks=all    │ topic "orders"           │  commits    │  (:8080)       │
└───────┬────────┘              │ 3 brokers + ZooKeeper    │  offsets    └───────┬────────┘
        │ SSE: /events          └──────────────────────────┘                     │ serves UI at /
        │ POST /burst, /pause, /resume                      SSE: /events (orders received + stats)
        └──────────────────────────────┐                  ┌─────────────────────┘
                                       ▼                  ▼
                                 ┌────────────────────────────┐
                                 │  Browser: Live Order Board │
                                 └────────────────────────────┘
```

## Quick start (Docker)

```bash
docker compose up --build        # ZooKeeper, 3 brokers, producer, consumer (add -d --wait to detach)
open http://localhost:8080
docker compose down -v           # stop everything and delete all data
```

The first build takes about two minutes. Nothing else is needed: the image builds the minikafka broker from its
GitHub source (`MINIKAFKA_REF`, default `main`, e.g. `MINIKAFKA_REF=<commit> docker compose up --build`)
together with both services.

## The page

- **Stats:** Total Orders · Total Revenue (₹) · Consumer Offset · Lag (green at 0, amber 1–20, red above 20).
- **Orders Sent (Producer)** on the left, **Orders Received (Consumer)** on the right, newest on top,
  each row `#orderId · item × qty · ₹price · offset N`. New rows flash briefly.
- **Send 50 orders**, **Pause** and **Resume** call the producer. The producer URL can be changed
  in the page (default from `PRODUCER_URL`, or `?producer=http://host:port`).
- **Connection dots** for each service: green while its event stream is connected. The page keeps
  reconnecting, and after a consumer restart it reloads the board from the consumer's snapshot and
  highlights the orders the consumer caught up on.

## Demo scenarios

1. **End-to-end flow.** An order appears in *Sent*, then within about a second in *Received*, with the
   same order id and offset.
2. **Consumer restart.**
   ```bash
   docker compose stop consumer    # the Consumer dot turns red, Sent keeps growing, Lag climbs
   sleep 15
   docker compose start consumer
   ```
   The consumer resumes from its committed offset (the *Received* header says "resumed from
   committed offset N"). It works through every order it missed, with no gaps or duplicates. Lag drains to
   0 and the totals are still correct.
3. **Burst.** Click **Send 50 orders** (it's clearest after **Pause**). Lag jumps to about 20–30 (amber or red) and
   drains back to 0 as the consumer works through them. All 50 arrive in order.
4. **Bonus: broker failover.** The topic has replication factor 3 and `min.insync.replicas` 2, so any
   one broker can be stopped mid-demo:
   ```bash
   docker compose exec broker2 minikafka topics describe --topic orders --bootstrap broker2:9092   # find the leader
   docker compose stop broker1     # if broker1 is the leader: a follower takes over, orders keep flowing
   docker compose start broker1    # it rejoins the ISR
   ```

`scripts/verify-demo.sh` checks scenarios 1–3 against a running stack (needs `curl`, `jq` and
`python3`; it stops and starts the consumer itself). It assumes the producer hasn't restarted since
the topic was created.

```
$ scripts/verify-demo.sh
Scenario 1: end-to-end flow
  PASS an order the producer just published is received (59ms later, budget ~1s)
  ...
Scenario 2: consumer restart (down for 15s)
  PASS it resumed from its committed offset 10 (it had processed offset 9 just before stopping)
  PASS the received list has no gaps or duplicates across the restart
  PASS totals match what the producer published ([29,8230])
  ...
Scenario 3: burst of 50 orders
  PASS lag rose visibly during the burst (peak 22)
  PASS all 50 orders arrived in order with no gaps or duplicates
all demo scenarios passed
```

## Running without Docker

The services build against a minikafka checkout next to this directory (`../minikafka`; override
with `-PminikafkaDir=...` or `MINIKAFKA_DIR`) through a Gradle composite build. They use its own
client library, `minikafka.client.MiniKafkaClient`. Any JDK that runs Gradle 9.7.1 works (tested with 26; the image uses 25).

```bash
# 1. a 3-broker cluster with ZooKeeper on localhost:2181 and brokers on 9092-9094
../minikafka/scripts/local-cluster.sh start

# 2. the services (two terminals)
BROKER_BOOTSTRAP=localhost:9092,localhost:9093,localhost:9094 ./gradlew :producer:run
BROKER_BOOTSTRAP=localhost:9092,localhost:9093,localhost:9094 ./gradlew :consumer:run

open http://localhost:8080

# checks: stop/start the consumer your own way, e.g. from installed distributions
./gradlew installDist
STOP_CONSUMER='kill $(cat consumer.pid)' \
START_CONSUMER='consumer/build/install/order-consumer/bin/order-consumer > consumer.log 2>&1 & echo $! > consumer.pid' \
  scripts/verify-demo.sh
```

A single broker works too: `minikafka zk` plus one `minikafka server`, and `BROKER_HOST`/`BROKER_PORT`.
The topic is then created with replication factor 1. Unit tests: `./gradlew test`.

## Configuration

| Variable | Service | Default |
|---|---|---|
| `BROKER_HOST` / `BROKER_PORT` | both | `localhost` / `9092` |
| `BROKER_BOOTSTRAP` | both | *(unset; `host:port,...`, overrides the two above)* |
| `TOPIC` | both | `orders` |
| `BROKER_SOCKET_TIMEOUT_MS` | both | producer `15000`, consumer `3000` (see Known limitations) |
| `PORT` | producer | `8081` |
| `PRODUCE_INTERVAL_MS` | producer | `1000` |
| `PRODUCE_TIMEOUT_MS` | producer | `12000` (how long an `acks=all` produce may wait) |
| `PORT` | consumer | `8080` |
| `CONSUMER_GROUP` | consumer | `order-board` |
| `PRODUCER_URL` | consumer (injected into the page) | `http://localhost:8081` |
| `PROCESS_DELAY_MS` | consumer | `100` (simulated work per order, which makes lag visible) |
| `POLL_INTERVAL_MS` | consumer | `100` (wait between empty fetches) |
| `FETCH_MAX_BYTES` | consumer | `1024` (about 7 orders per fetch, so the lag shown stays fresh) |

## HTTP APIs

**order-producer** (CORS open, because the page is served by the consumer):

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | `{"status":"ok"}` |
| `GET` | `/events` | SSE: a `snapshot` event (`{recent, status}`), then one `order` event per published order (`{order, offset}`) and a `status` event on pause/resume |
| `GET` | `/status` | `{paused, published, revenue, lastOrderId, lastOffset}` |
| `POST` | `/burst?count=N` | Publishes N orders back to back (default 50, clamped to 1–1000). Returns `{"published": N}` |
| `POST` | `/pause`, `/resume` | Stop / restart the one-per-interval generation |

**order-consumer:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | The Live Order Board page |
| `GET` | `/health` | `{"status":"ok"}` |
| `GET` | `/state` | `{recentOrders, totalOrders, totalRevenue, currentOffset, lag, highWatermark, committedOffset, resumedFrom, replayedOrders, skippedMessages}` |
| `GET` | `/events` | SSE: a `snapshot` event (the `/state` body), then one `order` event per received order (`{order: {order, offset}, stats: {totalOrders, totalRevenue, currentOffset, lag, highWatermark}}`) |

The message on topic `orders` is JSON (UTF-8), keyed by `orderId` as a string:
`{"orderId":101,"item":"Burger","qty":2,"price":240,"createdAt":"2026-09-30T10:15:30.000Z"}`.
The menu is Burger ₹120, Pizza ₹250, Pasta ₹180, Fries ₹80, Coffee ₹90 and Sandwich ₹110, with a quantity of 1–3.

## How it works

- **Broker client.** `common/…/OrderTopic.kt` is the thin `produce` / `fetch` / `commit` / `latestOffset`
  layer over minikafka's own `MiniKafkaClient`. That client handles bootstrap, routing to the
  partition leader, and retry with metadata refresh when a leader fails over. The producer
  creates the topic if it is missing (the consumer does too, whichever starts first). It uses 1 partition, so all orders are totally
  ordered, and replication factor min(3, live brokers).
- **Producer.** A single lock covers generating an order, producing it with `acks=all`, and
  broadcasting it. That keeps order ids, offsets and events in the same order even when a burst
  overlaps the ticker. An order id is only used once its produce succeeds.
- **Consumer offsets and totals.** The consumer commits after every order, storing the next offset to read in ZooKeeper through the
  broker's `OFFSET_COMMIT`. **Totals across restarts:** on startup it
  replays offsets 0 up to the committed offset *silently* to rebuild the totals and the recent list.
  It then consumes live from the committed offset, so each offset is applied exactly once per run.
  The UI shows where it resumed. This is the PRD's preferred option, with no extra state files. On
  `docker compose stop` it finishes the message in hand and commits before exiting. Messages that aren't valid orders are
  skipped but still committed past.
- **Lag** is the partition's high watermark minus the next offset to process. The high watermark comes back with every
  FETCH. While the consumer is down, the page also counts what the producer has published past the
  consumer's last offset, so the Lag card keeps climbing during an outage.
- **Web.** Both services use the JDK's built-in HTTP server and Server-Sent Events. There is no web framework, no
  frontend build step, and the page is plain HTML, CSS and JS.

Layout: `common/` (config, order model, broker client, SSE and HTTP helpers), `producer/`,
`consumer/` (+ `public/index.html`), `scripts/verify-demo.sh`, one `Dockerfile`, `docker-compose.yml`.

## Known limitations

- **Single partition, one consumer group.** Partition assignment, rebalancing and fan-out to a
  second group are out of scope. Because there's one partition, the order across the whole topic is the order in the log.
- **No "latest offset" API in minikafka.** Lag is derived from the high watermark returned by FETCH.
  The broker offers no long-polling either, so the consumer polls every `POLL_INTERVAL_MS` when idle.
- **At-least-once delivery.** If a produce is retried after a broker failure but the first attempt had
  in fact been written, the same order can land twice, at two offsets. The consumer counts every
  offset once, so it would show that order twice.
- **Totals are rebuilt by replay.** Startup time grows with the size of the topic. That's fine for a demo, but a
  real service would snapshot its state.
- **Producer counters are per run.** Order ids restart at 1 when the producer restarts, as the
  PRD asks. `verify-demo.sh`'s totals check assumes the producer hasn't restarted.
- **One client timeout for connect and read.** minikafka's client uses one timeout for both. After a
  broker is stopped, a retry can briefly still see it as leader and try to connect to its vanished
  container address. That connect is never refused, so it hangs until the timeout. The services
  therefore set short timeouts: 3 s for the consumer, whose fetches and commits answer immediately, and
  15 s for the producer, which must outlast a 12 s `acks=all` produce, which must outlast the brokers' 10 s
  replica lag time. A separate short connect timeout in minikafka's client would fix this at the source.
- **One image for everything.** The compose file builds a single image (broker and both services)
  instead of one per service, because parallel Gradle builds exhaust small Docker VMs. Brokers
  advertise their compose service names, so only the two services' HTTP ports are published to
  the host.
