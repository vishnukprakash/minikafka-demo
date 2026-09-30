package demo.consumer

import demo.common.Batch
import demo.common.DemoJson
import demo.common.Env
import demo.common.Http
import demo.common.OrderTopic
import demo.common.SseHub
import demo.common.send
import demo.common.sendJson
import minikafka.client.ClientConfig
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("demo.consumer")

/** The board page with the producer's URL injected (the page can still override it). */
fun renderIndexHtml(template: String, producerUrl: String): String =
    template.replace("\"__PRODUCER_URL__\"", DemoJson.encodeToString(producerUrl))

fun main() {
    val env = Env()
    val port = env.int("PORT", 8080)
    val topicName = env.str("TOPIC", "orders")
    val group = env.str("CONSUMER_GROUP", "order-board")
    val producerUrl = env.str("PRODUCER_URL", "http://localhost:8081")
    val processDelayMs = env.long("PROCESS_DELAY_MS", 100)
    val pollIntervalMs = env.long("POLL_INTERVAL_MS", 100)
    // Small fetches (~7 orders) keep the high watermark, and so the lag shown, fresh while a
    // backlog is being worked through.
    val fetchMaxBytes = env.int("FETCH_MAX_BYTES", 1024)
    val bootstrap = env.brokerBootstrap()

    // minikafka's client uses one timeout for both connecting and reading, and a stopped broker's
    // address can make a connect hang for that long. FETCH and OFFSET_COMMIT answer immediately
    // (no long-polling), so the consumer can afford a short one and fail over quickly.
    val clientConfig = ClientConfig(socketTimeoutMs = env.int("BROKER_SOCKET_TIMEOUT_MS", 3_000))
    val topic = OrderTopic(bootstrap, topicName, clientConfig)
    val hub = SseHub("consumer")
    val loop = ConsumerLoop(
        source = object : OrderSource {
            override fun committed(): Long? = topic.committed(group)
            override fun fetch(fromOffset: Long): Batch = topic.fetch(fromOffset, fetchMaxBytes)
            override fun commit(nextOffset: Long) = topic.commit(group, nextOffset)
        },
        listener = object : BoardListener {
            override fun onOrder(event: ReceivedEvent) = hub.broadcast("order", DemoJson.encodeToString(event))
            override fun onReset(state: BoardState) = hub.broadcast("snapshot", DemoJson.encodeToString(state))
        },
        processDelayMs = processDelayMs,
        pollIntervalMs = pollIntervalMs
    )

    val template = ConsumerLoop::class.java.getResource("/index.html")?.readText()
        ?: error("index.html missing from the classpath")
    val indexHtml = renderIndexHtml(template, producerUrl).toByteArray()

    Http(port)
        .get("/") { it.send(200, "text/html; charset=utf-8", indexHtml) }
        .get("/health") { it.sendJson(200, """{"status":"ok"}""") }
        .get("/state") { exchange -> exchange.sendJson(200, loop.locked { DemoJson.encodeToString(it.state()) }) }
        .get("/events") { exchange ->
            hub.serve(exchange) { client ->
                loop.locked { board ->
                    client.send("snapshot", DemoJson.encodeToString(board.state()))
                    hub.add(client)
                }
            }
        }
        .start()
    logger.info(
        "order-consumer listening on :{} (brokers {}, topic {}, group {})",
        port, bootstrap.joinToString(","), topicName, group
    )

    Runtime.getRuntime().addShutdownHook(Thread({
        logger.info("stopping: finishing the message in hand and committing")
        loop.stop()
    }, "consumer-shutdown"))

    OrderTopic.retrying("ensuring topic $topicName exists") { topic.ensureExists() }
    loop.run()
}
