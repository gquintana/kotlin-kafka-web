import com.salesforce.kafka.test.junit5.SharedKafkaTestResource
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.*

class RecordServiceTest {

	companion object {
		@JvmField
		@RegisterExtension
		val kafka = SharedKafkaTestResource()
		var producedRecords : List<RecordMetadata> = emptyList()

		val recordService: RecordService by lazy {
			val properties = Properties()
			properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.kafkaConnectString)
			RecordService(properties)
		}

		const val TOPIC = "test1"

		@JvmStatic
		@BeforeAll
		fun createRecords() {
			kafka.kafkaTestUtils.createTopic(TOPIC, 2, 1)
			producedRecords =
				kafka.kafkaTestUtils.getKafkaProducer(StringSerializer::class.java, StringSerializer::class.java)
					.use { producer ->
						val futures = (0..10).map { i ->
							val r = producer.send(ProducerRecord(TOPIC, "Key $i", "Message $i"))
							Thread.sleep(100)
							r
						}
						producer.flush()
						futures.map { it.get() }
					}
		}
	}

	@Test
	fun getRecordsWhenAll() {
		val records = recordService.getRecords(TOPIC, null, null)

		assertEquals(producedRecords.size, records.size)
	}

	@Test
	fun getRecordsWhenBeginOffset() {
		val records = recordService.getRecords(TOPIC, 5, null)

		val expectedRecords = producedRecords.filter { it.offset() >=5 }
		assertEquals(expectedRecords.size, records.size)
	}

	@Test
	fun getRecordsWhenBeginTimestamp() {
		val beginOffset = producedRecords[5].offset()
		val records = recordService.getRecords(TOPIC, null, beginOffset)

		val expectedRecords = producedRecords.filter { it.timestamp() >=beginOffset }
		assertEquals(expectedRecords.size, records.size)
	}
}
