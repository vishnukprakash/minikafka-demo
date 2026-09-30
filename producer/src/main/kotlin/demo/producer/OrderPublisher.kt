package demo.producer

import demo.common.DemoJson
import demo.common.OrderAtOffset
import kotlinx.serialization.Serializable
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Serializable
data class ProducerStatus(
    val paused: Boolean,
    val published: Long,
    val revenue: Long,
    val lastOrderId: Int,
    val lastOffset: Long
)

/**
 * Generates and publishes orders one at a time. Everything happens under one lock, so order ids,
 * topic offsets and [onPublished] callbacks are all in the same order even when the ticker and a
 * burst run concurrently. An order id is only used up once its produce succeeds.
 *
 * @param produce writes (key, JSON value) to the topic and returns the offset.
 */
class OrderPublisher(
    private val generator: OrderGenerator,
    private val produce: (key: String, value: String) -> Long,
    private val onPublished: (OrderAtOffset) -> Unit = {},
    private val recentCapacity: Int = 50
) {
    private val lock = ReentrantLock()
    private val recent = ArrayDeque<OrderAtOffset>()
    private var nextOrderId = 1
    private var published = 0L
    private var revenue = 0L
    private var lastOffset = -1L

    @Volatile
    var paused: Boolean = false

    fun publishOne(): OrderAtOffset = lock.withLock {
        val order = generator.order(nextOrderId)
        val offset = produce(order.orderId.toString(), DemoJson.encodeToString(order))
        nextOrderId++
        published++
        revenue += order.price
        lastOffset = offset
        val event = OrderAtOffset(order, offset)
        recent.addFirst(event)
        while (recent.size > recentCapacity) recent.removeLast()
        onPublished(event)
        event
    }

    fun status(): ProducerStatus = lock.withLock {
        ProducerStatus(paused, published, revenue, nextOrderId - 1, lastOffset)
    }

    /** Recently published orders, newest first. */
    fun recent(): List<OrderAtOffset> = lock.withLock { recent.toList() }

    /** Runs [block] with publishing blocked — to hand a new subscriber a snapshot with no event missed or repeated. */
    fun <T> locked(block: () -> T): T = lock.withLock(block)
}
