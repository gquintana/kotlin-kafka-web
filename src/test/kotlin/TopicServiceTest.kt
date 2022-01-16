import com.salesforce.kafka.test.junit5.SharedKafkaTestResource
import org.apache.kafka.clients.CommonClientConfigs
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.*

class TopicServiceTest {

	companion object {
		@JvmField
		@RegisterExtension
		val kafka = SharedKafkaTestResource()

		val topicService : TopicService by lazy {
			val properties = Properties()
			properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.kafkaConnectString)
			TopicService(properties)
		}

		@JvmStatic
		@BeforeAll
		fun createTopics() {
			kafka.kafkaTestUtils.createTopic("test1", 3, 1)
			kafka.kafkaTestUtils.createTopic("test2", 1, 1)
		}
	}

    @Test
    fun getTopics() {
		val topics = topicService.getTopics()

		assertEquals(setOf("test1","test2"), topics)
	}

    @Test
    fun getTopicWhenFound() {
		val topic = topicService.getTopic("test1")

		assertNotNull(topic)
		assertEquals("test1", topic!!.name)
		assertEquals(3, topic.partitions)
		assertEquals(1, topic.replicationFactor)
    }

	@Test
	fun getTopicWhenNotFound() {
		val topic = topicService.getTopic("test3")

		assertNull(topic)
	}
}
