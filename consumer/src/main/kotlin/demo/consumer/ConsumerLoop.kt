package demo.consumer

import demo.common.Batch
import demo.common.BrokerException
import demo.common.DemoJson
import demo.common.Message
import demo.common.OffsetOutOfRangeException
import demo.common.Order
import demo.common.OrderAtOffset
import kotlinx.serialization.SerializationException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** What the consumer needs from the broker, for one topic partition and one consumer group. */
interface OrderSource {
    /** The group's committed offset (the next offset to read), or null if it never committed. */
    fun committed(): Long?

    fun fetch(fromOffset: Long): Batch

    fun commit(nextOffset: Long)
}

/** Called under the loop's lock, in order. */
interface BoardListener {
    fun onOrder(event: ReceivedEvent)

    /** The board was rebuilt (startup, or the topic was recreated underneath the consumer). */
    fun onReset(state: BoardState)
}

/**
 * The consumer group member: on startup it replays offsets 0 until the group's committed offset
 * without emitting events (so the totals survive restarts), then consumes live from the committed
 * offset, emitting one event per order and committing after every message.
 *
 * Because the board is always rebuilt from the log and live consumption starts exactly at the
 * committed offset, each offset is applied to the board exactly once per run: a restart shows no
 * gaps and no duplicates, even if the process died between processing a message and committing it
 * (that message is then simply emitted live again after the restart).
 */
class ConsumerLoop(
    private val source: OrderSource,
    private val listener: BoardListener,
    private val processDelayMs: Long = 100,
    private val pollIntervalMs: Long = 100,
    private val errorBackoffMs: Long = 1_000,
    private val sleep: (Long) -> Unit = Thread::sleep
) {
    private val lock = ReentrantLock()
    private var board = OrderBoard() // guarded by lock; replaced wholesale by recover()
    private val stopped = CountDownLatch(1)

    @Volatile
    private var running = true

    /** The next offset to read. Only touched by the loop thread. */
    var nextOffset = 0L
        private set

    fun <T> locked(block: (OrderBoard) -> T): T = lock.withLock { block(board) }

    /**
     * Rebuilds the board from offset 0 up to the committed offset and positions the loop there.
     * If the log is shorter than the committed offset (the partition's high watermark is behind
     * it), live consumption resumes from the log end instead.
     */
    fun recover() {
        val committed = source.committed()
        val target = committed ?: 0
        val rebuilt = OrderBoard()
        var offset = 0L
        var highWatermark = 0L
        var replayed = 0L
        replay@ while (offset < target) {
            val batch = try {
                source.fetch(offset)
            } catch (e: OffsetOutOfRangeException) {
                break
            }
            highWatermark = batch.highWatermark
            if (batch.messages.isEmpty()) break
            for (m in batch.messages) {
                if (m.offset >= target) break@replay
                val order = parse(m)
                if (order != null) {
                    rebuilt.record(OrderAtOffset(order, m.offset), batch.highWatermark)
                    replayed++
                } else {
                    rebuilt.skip(m.offset, batch.highWatermark)
                }
                offset = m.offset + 1
            }
        }
        if (offset < target) {
            logger.warn("committed offset {} is past the log end {}; resuming from {}", target, offset, offset)
        }
        rebuilt.observeHighWatermark(highWatermark)
        rebuilt.committedOffset = committed ?: -1
        rebuilt.resumedFrom = offset
        rebuilt.replayedOrders = replayed
        nextOffset = offset
        lock.withLock {
            board = rebuilt
            listener.onReset(board.state())
        }
        if (committed == null) {
            logger.info("group has no committed offset; consuming from the beginning")
        } else {
            logger.info("resuming from committed offset {} (replayed {} orders to rebuild the totals)", offset, replayed)
        }
    }

    /** One FETCH and processing of whatever it returned; returns the number of messages handled. */
    fun pollOnce(): Int {
        val batch = source.fetch(nextOffset)
        if (batch.messages.isEmpty()) {
            lock.withLock { board.observeHighWatermark(batch.highWatermark) }
            return 0
        }
        var handled = 0
        for (m in batch.messages) {
            if (!running) break
            if (processDelayMs > 0) sleep(processDelayMs) // simulated per-order work
            val order = parse(m)
            lock.withLock {
                if (order != null) {
                    val received = OrderAtOffset(order, m.offset)
                    board.record(received, batch.highWatermark)
                    listener.onOrder(ReceivedEvent(received, board.stats()))
                } else {
                    board.skip(m.offset, batch.highWatermark)
                }
            }
            nextOffset = m.offset + 1
            handled++
            try {
                source.commit(nextOffset)
                lock.withLock { board.committedOffset = nextOffset }
            } catch (e: Exception) {
                // The next successful commit covers this offset too.
                logger.warn("committing offset {} failed: {}", nextOffset, e.message)
            }
        }
        return handled
    }

    /** Recovers, then polls until [stop] is called. Broker errors are logged and retried. */
    fun run() {
        try {
            while (running) {
                try {
                    recover()
                    break
                } catch (e: Exception) {
                    if (e !is IOException && e !is BrokerException) throw e
                    logger.warn("recovering failed ({}); retrying", e.message)
                    sleep(errorBackoffMs)
                }
            }
            while (running) {
                try {
                    if (pollOnce() == 0) sleep(pollIntervalMs)
                } catch (e: OffsetOutOfRangeException) {
                    logger.warn("{}: the topic was truncated or recreated; rebuilding from offset 0", e.message)
                    recover()
                } catch (e: IOException) {
                    logger.warn("fetch failed ({}); retrying", e.message)
                    sleep(errorBackoffMs)
                } catch (e: BrokerException) {
                    logger.warn("fetch failed ({}); retrying", e.message)
                    sleep(errorBackoffMs)
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            stopped.countDown()
        }
    }

    /** Asks [run] to finish after the message in hand (which is committed first) and waits up to [timeoutMs]. */
    fun stop(timeoutMs: Long = 5_000) {
        running = false
        stopped.await(timeoutMs, TimeUnit.MILLISECONDS)
    }

    private fun parse(m: Message): Order? = try {
        DemoJson.decodeFromString<Order>(m.value)
    } catch (e: SerializationException) {
        logger.warn("skipping offset {}: not an order ({})", m.offset, e.message?.lineSequence()?.first())
        null
    } catch (e: IllegalArgumentException) {
        logger.warn("skipping offset {}: not an order ({})", m.offset, e.message?.lineSequence()?.first())
        null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConsumerLoop::class.java)
    }
}
