package demo.common

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** The `orders` topic's message value (JSON, UTF-8); the message key is `orderId` as a string. */
@Serializable
data class Order(
    val orderId: Int,
    val item: String,
    val qty: Int,
    val price: Int,
    val createdAt: String
)

/** An order together with the offset it was written at in the `orders` topic. */
@Serializable
data class OrderAtOffset(val order: Order, val offset: Long)

val DemoJson = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    explicitNulls = false
}
