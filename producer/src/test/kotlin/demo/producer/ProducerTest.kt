package demo.producer

import demo.common.DemoJson
import demo.common.Order
import demo.common.OrderAtOffset
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.time.Instant
import kotlin.random.Random

class OrderGeneratorTest {
    private val generator = OrderGenerator(Random(42)) { Instant.parse("2026-09-30T10:15:30Z") }

    @Test
    fun `orders come from the menu and price is unit price times quantity`() {
        repeat(500) {
            val order = generator.order(it + 1)
            val unitPrice = OrderGenerator.MENU.getValue(order.item)
            assertTrue(order.qty in 1..3, "qty ${order.qty}")
            assertEquals(unitPrice * order.qty, order.price)
            assertEquals(it + 1, order.orderId)
        }
    }

    @Test
    fun `every menu item and quantity shows up`() {
        val orders = (1..500).map { generator.order(it) }
        assertEquals(OrderGenerator.MENU.keys, orders.map { it.item }.toSet())
        assertEquals(setOf(1, 2, 3), orders.map { it.qty }.toSet())
    }

    @Test
    fun `createdAt is ISO-8601 UTC with milliseconds`() {
        assertEquals("2026-09-30T10:15:30.000Z", generator.order(1).createdAt)
    }
}

class OrderPublisherTest {
    private val generator = OrderGenerator(Random(1)) { Instant.EPOCH }

    @Test
    fun `publishes sequential order ids keyed by id and reports the broker's offsets`() {
        val written = mutableListOf<Pair<String, String>>()
        val seen = mutableListOf<OrderAtOffset>()
        val publisher = OrderPublisher(generator, { k, v -> written += k to v; 100L + written.size - 1 }, { seen += it })

        repeat(3) { publisher.publishOne() }

        assertEquals(listOf("1", "2", "3"), written.map { it.first })
        assertEquals(listOf(1, 2, 3), written.map { DemoJson.decodeFromString<Order>(it.second).orderId })
        assertEquals(listOf(100L, 101L, 102L), seen.map { it.offset })
        val status = publisher.status()
        assertEquals(3, status.published)
        assertEquals(3, status.lastOrderId)
        assertEquals(102, status.lastOffset)
        assertEquals(seen.sumOf { it.order.price.toLong() }, status.revenue)
        assertEquals(seen.reversed(), publisher.recent())
    }

    @Test
    fun `a failed produce does not use up the order id`() {
        var fail = true
        val publisher = OrderPublisher(generator, { _, _ -> if (fail) throw IOException("broker down") else 0L })

        assertThrows<IOException> { publisher.publishOne() }
        fail = false
        assertEquals(1, publisher.publishOne().order.orderId)
        assertEquals(1, publisher.status().published)
    }

    @Test
    fun `keeps only the 50 most recent orders`() {
        var offset = 0L
        val publisher = OrderPublisher(generator, { _, _ -> offset++ })
        repeat(60) { publisher.publishOne() }
        assertEquals(50, publisher.recent().size)
        assertEquals(59L, publisher.recent().first().offset)
    }
}

class BurstCountTest {
    @Test
    fun `defaults to 50 and is clamped to 1 to 1000`() {
        assertEquals(50, burstCount(null))
        assertEquals(50, burstCount(""))
        assertEquals(7, burstCount("7"))
        assertEquals(1000, burstCount("5000"))
        assertEquals(1, burstCount("0"))
        assertThrows<IllegalArgumentException> { burstCount("lots") }
    }
}
