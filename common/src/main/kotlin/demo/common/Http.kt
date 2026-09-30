package demo.common

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.concurrent.Executors

private val logger = LoggerFactory.getLogger("demo.http")

/** A tiny router over the JDK's built-in HTTP server (no web framework needed for a demo). */
class Http(port: Int) {
    private val server: HttpServer = HttpServer.create(InetSocketAddress(port), 0).apply {
        // SSE handlers block for the life of their stream, so every request gets its own thread.
        executor = Executors.newCachedThreadPool()
    }
    private val routes = LinkedHashMap<Pair<String, String>, (HttpExchange) -> Unit>()

    init {
        server.createContext("/") { exchange -> dispatch(exchange) }
    }

    fun get(path: String, handler: (HttpExchange) -> Unit) = apply { routes["GET" to path] = handler }

    fun post(path: String, handler: (HttpExchange) -> Unit) = apply { routes["POST" to path] = handler }

    fun start() = server.start()

    private fun dispatch(exchange: HttpExchange) {
        try {
            // The Live Order Board is served by the consumer but also calls the producer.
            exchange.responseHeaders.apply {
                add("Access-Control-Allow-Origin", "*")
                add("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                add("Access-Control-Allow-Headers", "Content-Type")
            }
            val path = exchange.requestURI.path
            val method = exchange.requestMethod.uppercase()
            if (method == "OPTIONS") return exchange.send(204, null, null)
            val handler = routes[method to path]
            when {
                handler != null -> handler(exchange)
                routes.keys.any { it.second == path } -> exchange.sendJson(405, """{"error":"method not allowed"}""")
                else -> exchange.sendJson(404, """{"error":"not found"}""")
            }
        } catch (e: Exception) {
            logger.warn("{} {} failed", exchange.requestMethod, exchange.requestURI, e)
            runCatching { exchange.sendJson(500, """{"error":${DemoJson.encodeToString(e.message ?: e.toString())}}""") }
        } finally {
            exchange.close()
        }
    }
}

fun HttpExchange.send(status: Int, contentType: String?, body: ByteArray?) {
    if (contentType != null) responseHeaders.add("Content-Type", contentType)
    if (body == null || body.isEmpty()) {
        sendResponseHeaders(status, -1)
    } else {
        sendResponseHeaders(status, body.size.toLong())
        responseBody.use { it.write(body) }
    }
}

fun HttpExchange.sendJson(status: Int, json: String) = send(status, "application/json; charset=utf-8", json.toByteArray())

fun HttpExchange.query(): Map<String, String> =
    requestURI.rawQuery?.split('&')?.filter { it.isNotEmpty() }?.associate { pair ->
        val i = pair.indexOf('=')
        val key = if (i < 0) pair else pair.substring(0, i)
        val value = if (i < 0) "" else pair.substring(i + 1)
        URLDecoder.decode(key, Charsets.UTF_8) to URLDecoder.decode(value, Charsets.UTF_8)
    } ?: emptyMap()
