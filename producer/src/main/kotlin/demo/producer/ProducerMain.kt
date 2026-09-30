package demo.producer

import demo.common.DemoJson
import demo.common.Env
import demo.common.Http
import demo.common.OrderAtOffset
import demo.common.OrderTopic
import demo.common.SseHub
import demo.common.query
import demo.common.sendJson
import kotlinx.serialization.Serializable
import minikafka.client.ClientConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger("demo.producer")

@Serializable
private data class ProducerSnapshot(val recent: List<OrderAtOffset>, val status: ProducerStatus)

@Serializable
private data class BurstResult(val published: Int, val error: String? = null)

const val DEFAULT_BURST = 50
const val MAX_BURST = 1000

/** `count` query parameter of POST /burst: default [DEFAULT_BURST], clamped to 1..[MAX_BURST]. */
fun burstCount(raw: String?): Int {
    if (raw.isNullOrBlank()) return DEFAULT_BURST
    val n = raw.trim().toIntOrNull() ?: throw IllegalArgumentException("count must be an integer, got '$raw'")
    return n.coerceIn(1, MAX_BURST)
}

fun main() {
    val env = Env()
    val port = env.int("PORT", 8081)
    val topicName = env.str("TOPIC", "orders")
    val intervalMs = env.long("PRODUCE_INTERVAL_MS", 1000)
    val bootstrap = env.brokerBootstrap()

    // minikafka's client uses one timeout for both connecting and reading. A stopped broker's
    // address can make a connect hang for that long, so keep it short — but it must still exceed
    // the produce timeout (how long an acks=all produce may wait), which must exceed the brokers'
    // replica lag time (10s).
    val clientConfig = ClientConfig(
        socketTimeoutMs = env.int("BROKER_SOCKET_TIMEOUT_MS", 15_000),
        produceTimeoutMs = env.int("PRODUCE_TIMEOUT_MS", 12_000)
    )
    val topic = OrderTopic(bootstrap, topicName, clientConfig)
    val hub = SseHub("producer")
    val publisher = OrderPublisher(
        generator = OrderGenerator(),
        produce = topic::produce,
        onPublished = { hub.broadcast("order", DemoJson.encodeToString(it)) }
    )
    fun broadcastStatus() = publisher.locked { hub.broadcast("status", DemoJson.encodeToString(publisher.status())) }

    Http(port)
        .get("/health") { it.sendJson(200, """{"status":"ok"}""") }
        .get("/status") { it.sendJson(200, DemoJson.encodeToString(publisher.status())) }
        .get("/events") { exchange ->
            hub.serve(exchange) { client ->
                publisher.locked {
                    client.send("snapshot", DemoJson.encodeToString(ProducerSnapshot(publisher.recent(), publisher.status())))
                    hub.add(client)
                }
            }
        }
        .post("/burst") { exchange ->
            val count = try {
                burstCount(exchange.query()["count"])
            } catch (e: IllegalArgumentException) {
                return@post exchange.sendJson(400, """{"error":${DemoJson.encodeToString(e.message)}}""")
            }
            var published = 0
            try {
                repeat(count) {
                    publisher.publishOne()
                    published++
                }
                logger.info("burst: published {} orders", published)
                exchange.sendJson(200, DemoJson.encodeToString(BurstResult(published)))
            } catch (e: Exception) {
                logger.warn("burst stopped after {} of {} orders: {}", published, count, e.message)
                exchange.sendJson(503, DemoJson.encodeToString(BurstResult(published, e.message ?: e.toString())))
            }
        }
        .post("/pause") { exchange ->
            publisher.paused = true
            broadcastStatus()
            logger.info("automatic order generation paused")
            exchange.sendJson(200, DemoJson.encodeToString(publisher.status()))
        }
        .post("/resume") { exchange ->
            publisher.paused = false
            broadcastStatus()
            logger.info("automatic order generation resumed")
            exchange.sendJson(200, DemoJson.encodeToString(publisher.status()))
        }
        .start()
    logger.info("order-producer listening on :{} (brokers {}, topic {})", port, bootstrap.joinToString(","), topicName)

    OrderTopic.retrying("ensuring topic $topicName exists") { topic.ensureExists() }
    logger.info("publishing one order every {}ms to {}", intervalMs, topicName)

    Executors.newSingleThreadScheduledExecutor { Thread(it, "order-ticker") }.scheduleWithFixedDelay({
        if (!publisher.paused) {
            try {
                val sent = publisher.publishOne()
                logger.debug("published order {} at offset {}", sent.order.orderId, sent.offset)
            } catch (e: Exception) {
                logger.warn("publishing failed (will retry on the next tick): {}", e.message)
            }
        }
    }, 0, intervalMs, TimeUnit.MILLISECONDS)
}
