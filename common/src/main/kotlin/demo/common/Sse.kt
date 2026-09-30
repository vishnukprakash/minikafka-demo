package demo.common

import com.sun.net.httpserver.HttpExchange
import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** One connected browser's Server-Sent Events stream. */
class SseClient(private val out: OutputStream) {
    private val closed = CountDownLatch(1)

    val isOpen: Boolean get() = closed.count > 0

    /** Sends one event; false (and the client is closed) if the browser has gone away. */
    fun send(event: String, data: String): Boolean = write("event: $event\ndata: $data\n\n")

    fun comment(text: String): Boolean = write(": $text\n\n")

    @Synchronized
    private fun write(frame: String): Boolean {
        if (!isOpen) return false
        return try {
            out.write(frame.toByteArray())
            out.flush()
            true
        } catch (_: IOException) {
            close()
            false
        }
    }

    fun close() {
        runCatching { out.close() }
        closed.countDown()
    }

    fun awaitClosed() = closed.await()
}

/**
 * The set of connected SSE clients. Callers that need "snapshot, then every later event, with
 * nothing missed or repeated" send the snapshot and [add] the client under the same lock they
 * [broadcast] under.
 */
class SseHub(name: String) {
    private val clients = CopyOnWriteArraySet<SseClient>()

    init {
        // Heartbeats keep proxies from timing the stream out and detect browsers that went away.
        Executors.newSingleThreadScheduledExecutor { Thread(it, "$name-sse-heartbeat").apply { isDaemon = true } }
            .scheduleWithFixedDelay({ clients.forEach { if (!it.comment("ping")) clients.remove(it) } }, 10, 10, TimeUnit.SECONDS)
    }

    val size: Int get() = clients.size

    fun add(client: SseClient) {
        clients.add(client)
    }

    fun broadcast(event: String, data: String) {
        clients.forEach { if (!it.send(event, data)) clients.remove(it) }
    }

    /**
     * Turns [exchange] into an event stream, lets [onOpen] send the initial events and register the
     * client, then blocks this handler thread until the browser disconnects.
     */
    fun serve(exchange: HttpExchange, onOpen: (SseClient) -> Unit) {
        exchange.responseHeaders.apply {
            add("Content-Type", "text/event-stream; charset=utf-8")
            add("Cache-Control", "no-cache")
            add("Connection", "keep-alive")
        }
        exchange.sendResponseHeaders(200, 0)
        val client = SseClient(exchange.responseBody)
        try {
            onOpen(client)
            client.awaitClosed()
        } finally {
            clients.remove(client)
            client.close()
        }
    }
}
