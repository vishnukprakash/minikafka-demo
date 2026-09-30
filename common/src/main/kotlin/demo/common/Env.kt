package demo.common

import minikafka.client.HostPort

/** Environment-variable configuration; [lookup] is injectable for tests. */
class Env(private val lookup: (String) -> String? = System::getenv) {
    fun str(name: String, default: String): String = lookup(name)?.trim()?.takeIf { it.isNotEmpty() } ?: default

    fun int(name: String, default: Int): Int = lookup(name)?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toIntOrNull() ?: throw IllegalArgumentException("$name must be an integer, got '$it'")
    } ?: default

    fun long(name: String, default: Long): Long = lookup(name)?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull() ?: throw IllegalArgumentException("$name must be an integer, got '$it'")
    } ?: default

    /**
     * Brokers to bootstrap from. `BROKER_BOOTSTRAP` (`host:port,host:port,...`) wins; otherwise
     * `BROKER_HOST`:`BROKER_PORT` (default `localhost:9092`, the broker's default port). One broker
     * is enough — the client discovers the rest of the cluster from its METADATA — but listing
     * several lets a service start while one of them is down.
     */
    fun brokerBootstrap(): List<HostPort> {
        val list = str("BROKER_BOOTSTRAP", "")
        if (list.isNotEmpty()) return HostPort.parseList(list)
        return listOf(HostPort(str("BROKER_HOST", "localhost"), int("BROKER_PORT", DEFAULT_BROKER_PORT)))
    }

    companion object {
        const val DEFAULT_BROKER_PORT = 9092
    }
}
