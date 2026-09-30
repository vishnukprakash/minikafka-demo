package demo.consumer

import demo.common.OrderAtOffset
import kotlinx.serialization.Serializable

@Serializable
data class Stats(
    val totalOrders: Long,
    val totalRevenue: Long,
    /** Offset of the last message processed, -1 before the first. */
    val currentOffset: Long,
    /** Messages in the topic not yet processed: high watermark − (currentOffset + 1). */
    val lag: Long,
    val highWatermark: Long
)

/** One SSE `order` event: the order just received, with the stats after applying it. */
@Serializable
data class ReceivedEvent(val order: OrderAtOffset, val stats: Stats)

/** GET /state, and the SSE `snapshot` event. */
@Serializable
data class BoardState(
    /** Newest first. */
    val recentOrders: List<OrderAtOffset>,
    val totalOrders: Long,
    val totalRevenue: Long,
    val currentOffset: Long,
    val lag: Long,
    val highWatermark: Long,
    /** The next offset this group has committed, -1 if none yet. */
    val committedOffset: Long,
    /** The committed offset this run resumed live consumption from. */
    val resumedFrom: Long,
    /** Orders replayed (offsets 0 until [resumedFrom]) at startup to rebuild the totals. */
    val replayedOrders: Long,
    /** Messages that were not valid order JSON (skipped, but still committed past). */
    val skippedMessages: Long
)

/** The consumer's in-memory view of the topic. Not thread-safe: [ConsumerLoop] guards it with its lock. */
class OrderBoard(private val capacity: Int = 50) {
    private val recent = ArrayDeque<OrderAtOffset>()
    private var totalOrders = 0L
    private var totalRevenue = 0L
    private var currentOffset = -1L
    private var highWatermark = 0L
    private var skipped = 0L

    var committedOffset = -1L
    var resumedFrom = 0L
    var replayedOrders = 0L

    fun record(order: OrderAtOffset, highWatermark: Long) {
        recent.addFirst(order)
        while (recent.size > capacity) recent.removeLast()
        totalOrders++
        totalRevenue += order.order.price
        advance(order.offset, highWatermark)
    }

    /** A message that is not an order: move past it without counting it. */
    fun skip(offset: Long, highWatermark: Long) {
        skipped++
        advance(offset, highWatermark)
    }

    private fun advance(offset: Long, highWatermark: Long) {
        currentOffset = offset
        this.highWatermark = maxOf(this.highWatermark, highWatermark)
    }

    fun observeHighWatermark(highWatermark: Long) {
        this.highWatermark = maxOf(this.highWatermark, highWatermark)
    }

    fun lag(): Long = (highWatermark - (currentOffset + 1)).coerceAtLeast(0)

    fun stats() = Stats(totalOrders, totalRevenue, currentOffset, lag(), highWatermark)

    fun state() = BoardState(
        recent.toList(), totalOrders, totalRevenue, currentOffset, lag(), highWatermark,
        committedOffset, resumedFrom, replayedOrders, skipped
    )
}
