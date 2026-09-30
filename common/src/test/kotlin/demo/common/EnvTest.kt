package demo.common

import minikafka.client.HostPort
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class EnvTest {
    private fun env(vararg pairs: Pair<String, String>) = Env(mapOf(*pairs)::get)

    @Test
    fun `defaults to the broker's default address`() {
        assertEquals(listOf(HostPort("localhost", 9092)), env().brokerBootstrap())
    }

    @Test
    fun `BROKER_HOST and BROKER_PORT`() {
        assertEquals(listOf(HostPort("broker1", 9093)), env("BROKER_HOST" to "broker1", "BROKER_PORT" to "9093").brokerBootstrap())
    }

    @Test
    fun `BROKER_BOOTSTRAP wins over BROKER_HOST`() {
        val e = env("BROKER_BOOTSTRAP" to "a:1, b:2", "BROKER_HOST" to "ignored")
        assertEquals(listOf(HostPort("a", 1), HostPort("b", 2)), e.brokerBootstrap())
    }

    @Test
    fun `blank values fall back to defaults and bad numbers are rejected`() {
        assertEquals(8080, env("PORT" to "  ").int("PORT", 8080))
        assertThrows<IllegalArgumentException> { env("PORT" to "eighty").int("PORT", 8080) }
    }
}
