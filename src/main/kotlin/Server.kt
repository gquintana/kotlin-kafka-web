import io.ktor.application.*
import io.ktor.features.*
import io.ktor.html.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.serialization.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.html.*
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

fun HTML.index() {
	head {
		title("Hello from Ktor!")
	}
	body {
		div {
			+"Hello from Ktor"
		}
	}
}

fun main() {
	val properties = loadProperties()
	val server = Server(properties)
	server.run()
}

fun loadProperties(): Properties {
	val properties = Properties()
	val propertiesReader = Files.newBufferedReader(Path.of("kafka.properties"))
	propertiesReader.use {
		properties.load(propertiesReader)
	}
	return properties
}

class Server(properties: Properties) : AutoCloseable {
	val topicService = TopicService(properties)
	val recordService = RecordService(properties)
	val engine = embeddedServer(Netty, port = 8080, host = "127.0.0.1") {
		install(ContentNegotiation) {
			json(Json {
				prettyPrint = true
				isLenient = true
			})
		}
		routing {
			get("/") {
				call.respondHtml(HttpStatusCode.OK, HTML::index)
			}
			get("/topics") {
				call.respond(topicService.getTopics())
			}
			get("/topics/{topic}") {
				val topicName = call.parameters["topic"]!!
				val topic = topicService.getTopic(topicName)
				if (topic == null) {
					call.respond(HttpStatusCode.NotFound, "Topic $topicName not found")
				} else {
					call.respond(topic)
				}
			}
		}
	}

	fun run() {
		engine.start(wait = true)
		close()
	}
	fun start() {
		engine.start(wait = false)
	}


	override fun close() {
		topicService.close()
		recordService.close()
	}
}

