package demo.consumer

import demo.common.Batch
import demo.common.DemoJson
import demo.common.Message
import demo.common.OffsetOutOfRangeException
import demo.common.Order
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** An in-memory single-partition topic with one consumer group's committed offset. */
private class FakeTopic(var committed: Long? = null, private val maxPerFetch: Int = 3) : OrderSource {
    val log = mutableListOf<String>()
    val commits = mutableListOf<Long>()

    fun append(vararg orderIds: Int) = orderIds.forEach { log += DemoJson.encodeToString(order(it)) }

    override fun committed(): Long? = committed

    override fun fetch(fromOffset: Long): Batch {
        if (fromOffset > log.size) throw OffsetOutOfRangeException(fromOffset, log.size.toLong())
        val messages = (fromOffset until minOf(log.size.toLong(), fromOffset + maxPerFetch))
            .map { Message(it, null, log[it.toInt()]) }
        return Batch(messages, log.size.toLong())
    }

    override fun commit(nextOffset: Long) {
        commits += nextOffset
        committed = nextOffset
    }

    companion object {
        fun order(id: Int) = Order(id, "Pizza", 2, 500, "2026-09-30T10:15:30.000Z")
    }
}

private class Recorder : BoardListener {
    val orders = mutableListOf<ReceivedEvent>()
    val resets = mutableListOf<BoardState>()
    override fun onOrder(event: ReceivedEvent) {
        orders += event
    }

    override fun onReset(state: BoardState) {
        resets += state
    }
}

class ConsumerLoopTest {
    private fun loop(topic: FakeTopic, recorder: Recorder) =
        ConsumerLoop(topic, recorder, processDelayMs = 0, pollIntervalMs = 0, sleep = {})

    private fun ConsumerLoop.drain() {
        do { val handled = pollOnce() } while (handled > 0)
    }

    @Test
    fun `a new group consumes from the beginning and commits after every message`() {
        val topic = FakeTopic().apply { append(1, 2, 3, 4) }
        val recorder = Recorder()
        val loop = loop(topic, recorder)

        loop.recover()
        loop.drain()

        assertEquals(listOf(0L, 1L, 2L, 3L), recorder.orders.map { it.order.offset })
        assertEquals(listOf(1L, 2L, 3L, 4L), topic.commits)
        val state = loop.locked { it.state() }
        assertEquals(4, state.totalOrders)
        assertEquals(2000, state.totalRevenue)
        assertEquals(3, state.currentOffset)
        assertEquals(0, state.lag)
        assertEquals(4, state.committedOffset)
        assertEquals(listOf(4, 3, 2, 1), state.recentOrders.map { it.order.orderId })
    }

    @Test
    fun `a restart replays up to the committed offset silently, then resumes live with no gaps or duplicates`() {
        val topic = FakeTopic(committed = 5).apply { append(*(1..8).toList().toIntArray()) }
        val recorder = Recorder()
        val loop = loop(topic, recorder)

        loop.recover()
        val afterRecovery = recorder.resets.single()
        assertEquals(5, afterRecovery.resumedFrom)
        assertEquals(5, afterRecovery.replayedOrders)
        assertEquals(5, afterRecovery.totalOrders)
        assertEquals(4, afterRecovery.currentOffset)
        assertEquals(3, afterRecovery.lag) // offsets 5, 6, 7 are still to be processed
        assertEquals(listOf(5, 4, 3, 2, 1), afterRecovery.recentOrders.map { it.order.orderId })
        assertTrue(recorder.orders.isEmpty(), "replayed orders are not emitted live")

        loop.drain()
        assertEquals(listOf(5L, 6L, 7L), recorder.orders.map { it.order.offset })
        val state = loop.locked { it.state() }
        assertEquals(8, state.totalOrders)
        assertEquals((7L downTo 0L).toList(), state.recentOrders.map { it.offset })
        assertEquals(0, state.lag)
    }

    @Test
    fun `lag counts messages behind the high watermark`() {
        val topic = FakeTopic(maxPerFetch = 10).apply { append(1, 2, 3, 4, 5) }
        val recorder = Recorder()
        val loop = loop(topic, recorder)
        loop.recover()
        loop.pollOnce()
        assertEquals(listOf(4L, 3L, 2L, 1L, 0L), recorder.orders.map { it.stats.lag })
    }

    @Test
    fun `messages that are not orders are skipped but committed past`() {
        val topic = FakeTopic().apply {
            append(1)
            log += "hello"
            append(3)
        }
        val recorder = Recorder()
        val loop = loop(topic, recorder)
        loop.recover()
        loop.drain()
        assertEquals(listOf(1, 3), recorder.orders.map { it.order.order.orderId })
        assertEquals(3L, topic.committed)
        assertEquals(1, loop.locked { it.state().skippedMessages })
    }

    @Test
    fun `a committed offset past the log end resumes from the log end`() {
        val topic = FakeTopic(committed = 10).apply { append(1, 2) }
        val recorder = Recorder()
        val loop = loop(topic, recorder)
        loop.recover()
        assertEquals(2L, loop.nextOffset)
        assertEquals(2, recorder.resets.single().totalOrders)
        topic.append(3)
        loop.drain()
        assertEquals(listOf(2L), recorder.orders.map { it.order.offset })
    }

    @Test
    fun `keeps only the 50 most recent orders but totals everything`() {
        val topic = FakeTopic(maxPerFetch = 100).apply { append(*(1..70).toList().toIntArray()) }
        val loop = loop(topic, Recorder())
        loop.recover()
        loop.drain()
        val state = loop.locked { it.state() }
        assertEquals(50, state.recentOrders.size)
        assertEquals(70, state.recentOrders.first().order.orderId)
        assertEquals(70, state.totalOrders)
    }
}

class IndexHtmlTest {
    @Test
    fun `the producer URL is injected as a JSON string`() {
        val html = renderIndexHtml("""const u = "__PRODUCER_URL__";""", "http://localhost:8081")
        assertEquals("""const u = "http://localhost:8081";""", html)
    }

    @Test
    fun `the shipped page has the placeholder`() {
        val page = ConsumerLoop::class.java.getResource("/index.html")!!.readText()
        assertTrue(page.contains("\"__PRODUCER_URL__\""))
    }
}
