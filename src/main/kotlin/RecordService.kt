import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
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

	private val consumer: Consumer<String, String> by lazy {
		val consumerProps = Properties()
		consumerProps.putAll(properties)
		consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
		consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
		KafkaConsumer(consumerProps, StringDeserializer(), StringDeserializer())
	}


	fun getRecords(topic: String, beginOffset: Long?, beginTimestamp: Long?): List<Record> {
		val partitions = consumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
		consumer.assign(partitions)
		try {
			when {
				beginOffset != null -> partitions.forEach { consumer.seek(it, beginOffset) }
				beginTimestamp != null -> {
					val partitionTimestamps = partitions.associateWith { beginTimestamp }
					val partitionOffsets = consumer.offsetsForTimes(partitionTimestamps)
					partitionOffsets.filter { it.value != null }.forEach { consumer.seek(it.key, it.value.offset()) }
				}
				else -> consumer.seekToBeginning(partitions)
			}
			var records : List<Record>
			var pollCount = 0
			do {
				records = consumer.poll(Duration.ofMillis(100))
					.map { Record(it.offset(), it.timestamp(), it.key(), it.value()) }
					.toList()
				pollCount++
			} while (records.isEmpty() && pollCount < 10)
			return records
		} finally {
		    consumer.assign(emptyList())
		}
	}

	override fun close() {
		consumer.close()
	}
}
