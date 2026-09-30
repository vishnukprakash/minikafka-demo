package demo.producer

import demo.common.Order
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.random.Random

/** Random food orders from a fixed menu (prices in ₹). */
class OrderGenerator(
    private val random: Random = Random.Default,
    private val clock: () -> Instant = Instant::now
) {
    fun order(orderId: Int): Order {
        val (item, unitPrice) = MENU.entries.elementAt(random.nextInt(MENU.size)).toPair()
        val qty = random.nextInt(1, 4)
        return Order(orderId, item, qty, unitPrice * qty, TIMESTAMP.format(clock()))
    }

    companion object {
        val MENU: Map<String, Int> = linkedMapOf(
            "Burger" to 120,
            "Pizza" to 250,
            "Pasta" to 180,
            "Fries" to 80,
            "Coffee" to 90,
            "Sandwich" to 110
        )

        /** ISO-8601 in UTC, always with milliseconds (e.g. `2026-09-30T10:15:30.000Z`). */
        private val TIMESTAMP: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)
    }
}
