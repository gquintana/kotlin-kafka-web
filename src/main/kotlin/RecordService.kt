import kotlinx.serialization.Serializable
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

@Serializable
data class Record(val offset: Long, val timestamp: Long, val key: String?, val value: String?)

class RecordService(properties: Properties) : AutoCloseable {
	private val consumerPool = GenericObjectPool(
		PooledConsumerFactory(properties),
		createPoolConfig()
	)


	fun getRecords(topic: String, beginOffset: Long?, beginTimestamp: Long?): List<Record> {
		val consumer = consumerPool.borrowObject(1000L)
		try {
			val partitions = consumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
			consumer.assign(partitions)
			when {
				beginOffset != null -> partitions.forEach { consumer.seek(it, beginOffset) }
				beginTimestamp != null -> {
					val partitionTimestamps = partitions.associateWith { beginTimestamp }
					val partitionOffsets = consumer.offsetsForTimes(partitionTimestamps)
					partitionOffsets.filter { it.value != null }
						.forEach { consumer.seek(it.key, it.value.offset()) }
				}
				else -> consumer.seekToBeginning(partitions)
			}
			var records: List<Record>
			var pollCount = 0
			do {
				records = consumer.poll(Duration.ofMillis(100))
					.map { Record(it.offset(), it.timestamp(), it.key(), it.value()) }
					.toList()
				pollCount++
			} while (records.isEmpty() && pollCount < 10)
			return records
		} finally {
			consumerPool.returnObject(consumer)
		}
	}

	override fun close() {
		consumerPool.close()
	}
}

private class PooledConsumerFactory(val properties: Properties) : BasePooledObjectFactory<Consumer<String, String>>() {
	var consumerId = 1

	override fun create(): Consumer<String, String> {
		val consumerProps = Properties()
		consumerProps.putAll(properties)
		consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
		consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
		consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "pooled-consumer-" + (consumerId++))
		return KafkaConsumer(consumerProps, StringDeserializer(), StringDeserializer())
	}

	override fun wrap(consumer: Consumer<String, String>?): PooledObject<Consumer<String, String>> {
		return DefaultPooledObject(consumer)
	}

	override fun passivateObject(pooledConsumer: PooledObject<Consumer<String, String>>?) {
		pooledConsumer!!.`object`.assign(emptyList())
	}

	override fun destroyObject(pooledConsumer: PooledObject<Consumer<String, String>>?) {
		pooledConsumer!!.`object`.close()
	}
}

private fun createPoolConfig(): GenericObjectPoolConfig<Consumer<String, String>> {
	val poolConfig = GenericObjectPoolConfig<Consumer<String, String>>()
	poolConfig.maxIdle = 3
	poolConfig.minIdle = 1
	poolConfig.maxTotal = 10
	return poolConfig
}
