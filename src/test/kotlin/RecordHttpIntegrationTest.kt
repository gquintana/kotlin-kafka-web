import io.ktor.client.*
import io.ktor.client.features.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RecordHttpIntegrationTest {
	val client = HttpClient {
		install(JsonFeature)
		defaultRequest {
			port = 8080
		}
	}

	@Test
	fun getRecordsWhenAll() {
		runBlocking {
			val records:List<Record> = client.get("/topics/test1/records")
			assertEquals(10, records.size)
		}
	}

	@Test
	fun getRecordsWhenBeginOffset() {
		runBlocking {
			val records:List<Record> = client.get("/topics/test1/records?beginOffset=3")
			assertEquals(2, records.size)
		}
	}

}
