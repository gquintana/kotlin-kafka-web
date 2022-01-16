import io.ktor.client.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TopicHttpIntegrationTest {
	val client = HttpClient {
		install(JsonFeature)
		defaultRequest {
			port = 8080
		}
	}

	@Test
	fun getTopics() {
		runBlocking {
			val topics: List<String> = client.get(path = "/topics")

			assertEquals(listOf("test1", "test2"), topics.sorted())
		}
	}

	@Test
	fun getTopicWhenFound() {
		runBlocking {
			val topic: Topic? = client.get(path = "/topics/test1")

			assertNotNull(topic)
			assertEquals("test1", topic!!.name)
			assertEquals(3, topic.partitions)
			assertEquals(1, topic.replicationFactor)
		}
	}

	@Test
	fun getTopicWhenNotFound() {
		runBlocking {
			try {
				val response: HttpResponse = client.get("/topics/test3")
				fail("Expect exception")
			} catch (e: ClientRequestException) {
				assertEquals(HttpStatusCode.NotFound, e.response.status)
			}
		}
	}

}
