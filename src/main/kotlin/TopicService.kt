import kotlinx.serialization.Serializable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import java.util.*
import java.util.concurrent.ExecutionException

@Serializable
data class Topic(val name: String, val partitions: Int, val replicationFactor: Int)

class TopicService(properties: Properties) : AutoCloseable {
	private val adminClient: AdminClient by lazy {
		AdminClient.create(properties)
	}

	fun getTopics(): Set<String> = adminClient.listTopics().names().get()

	fun getTopic(name: String): Topic? =
		try {
			val topicsDesc = adminClient.describeTopics(listOf(name)).all().get()
			topicsDesc[name]?.let { Topic(name, it.partitions().size, it.partitions()[0].replicas().size) }
		} catch (e: ExecutionException) {
			if (e.cause is UnknownTopicOrPartitionException) {
				null
			} else {
				throw e
			}
		}


	override fun close() {
		adminClient.close()
	}
}
