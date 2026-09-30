package demo.common

import minikafka.client.ClientConfig
import minikafka.client.HostPort
import minikafka.client.MiniKafkaClient
import minikafka.proto.ErrorCodes
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException

open class BrokerException(message: String) : RuntimeException(message)

/** FETCH asked for an offset past the log end — e.g. a committed offset from before the topic was recreated. */
class OffsetOutOfRangeException(val offset: Long, val highWatermark: Long) :
    BrokerException("offset $offset is out of range (high watermark $highWatermark)")

data class Message(val offset: Long, val key: String?, val value: String)

/** Messages read by one FETCH, plus the partition's high watermark (one past the last committed-to-ISR offset). */
data class Batch(val messages: List<Message>, val highWatermark: Long)

/**
 * The thin broker client both services share: the PRD's `produce` / `fetch` / `commit` /
 * `latestOffset` on top of minikafka's own [MiniKafkaClient], which already handles bootstrap,
 * leader routing, and retry-with-metadata-refresh across broker failover.
 *
 * The demo uses a single-partition topic (partition [PARTITION]) so that every order is totally
 * ordered; multiple partitions and consumer rebalancing are PRD non-goals.
 */
class OrderTopic(private val client: MiniKafkaClient, val topic: String) : Closeable {
    constructor(bootstrap: List<HostPort>, topic: String, config: ClientConfig = ClientConfig()) :
        this(MiniKafkaClient(bootstrap, config), topic)

    /**
     * Creates [topic] (1 partition, replication factor min([maxReplicationFactor], live brokers))
     * unless it exists, then waits until its partition has a leader. Safe to race with the other
     * service doing the same (TOPIC_ALREADY_EXISTS is fine).
     */
    fun ensureExists(maxReplicationFactor: Int = 3, timeoutMs: Long = 30_000) {
        val metadata = client.metadata()
        if (metadata.topic(topic) == null) {
            val rf = minOf(maxReplicationFactor, metadata.brokers.size).coerceAtLeast(1)
            val code = client.createTopic(topic, 1, rf)
            when (code) {
                ErrorCodes.NONE -> logger.info("created topic {} (1 partition, replication factor {})", topic, rf)
                ErrorCodes.TOPIC_ALREADY_EXISTS -> {}
                else -> throw BrokerException("creating topic $topic failed: ${errorName(code)}")
            }
        }
        val deadline = System.currentTimeMillis() + timeoutMs
        while (true) {
            val leader = client.metadata().topic(topic)?.partition(PARTITION)?.leader ?: -1
            if (leader >= 0) return
            if (System.currentTimeMillis() > deadline) throw BrokerException("topic $topic has no partition leader after ${timeoutMs}ms")
            Thread.sleep(200)
        }
    }

    /** Publishes with acks=all; returns the offset the message was written at. */
    fun produce(key: String, value: String): Long {
        val response = client.produce(topic, key.toByteArray(), value.toByteArray(), MiniKafkaClient.ACKS_ALL, PARTITION)
        if (response.errorCode != ErrorCodes.NONE) throw BrokerException("produce to $topic failed: ${errorName(response.errorCode)}")
        return response.offset
    }

    /** Messages from [fromOffset] up to the high watermark (at most about [maxBytes] of them). */
    fun fetch(fromOffset: Long, maxBytes: Int = 64 * 1024): Batch {
        val response = client.fetch(topic, PARTITION, fromOffset, maxBytes)
        when (response.errorCode) {
            ErrorCodes.NONE -> {}
            ErrorCodes.OFFSET_OUT_OF_RANGE -> throw OffsetOutOfRangeException(fromOffset, response.highWatermark)
            else -> throw BrokerException("fetch from $topic failed: ${errorName(response.errorCode)}")
        }
        val messages = response.records.map { Message(it.offset, it.key?.let(::String), String(it.value)) }
        return Batch(messages, response.highWatermark)
    }

    /** Commits [nextOffset] — the next offset [group] will read, Kafka's convention — to ZooKeeper via the broker. */
    fun commit(group: String, nextOffset: Long) {
        val code = client.commitOffset(group, topic, PARTITION, nextOffset)
        if (code != ErrorCodes.NONE) throw BrokerException("commit for $group failed: ${errorName(code)}")
    }

    /** The offset [group] committed last, or null if it never committed. */
    fun committed(group: String): Long? = client.fetchOffset(group, topic, PARTITION).takeIf { it >= 0 }

    /**
     * The newest offset a consumer can read, or -1 if there is none. minikafka has no dedicated
     * API for this; it is derived from a FETCH's high watermark.
     */
    fun latestOffset(): Long = client.fetch(topic, PARTITION, 0, 1).highWatermark - 1

    override fun close() = client.close()

    companion object {
        const val PARTITION = 0
        private val logger = LoggerFactory.getLogger(OrderTopic::class.java)

        private val errorNames: Map<Short, String> = ErrorCodes::class.java.declaredFields
            .filter { it.type == Short::class.javaPrimitiveType }
            .associate { (it.get(null) as Short) to it.name }

        fun errorName(code: Short): String = errorNames[code] ?: "error $code"

        /** Runs [block] until it succeeds, logging and retrying every [delayMs] (brokers may still be starting). */
        fun <T> retrying(what: String, delayMs: Long = 2_000, block: () -> T): T {
            while (true) {
                try {
                    return block()
                } catch (e: IOException) {
                    logger.warn("{}: broker unreachable ({}); retrying in {}ms", what, e.message, delayMs)
                } catch (e: BrokerException) {
                    logger.warn("{}: {}; retrying in {}ms", what, e.message, delayMs)
                }
                Thread.sleep(delayMs)
            }
        }
    }
}
