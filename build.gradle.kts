import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.31"
	kotlin("plugin.serialization") version "1.5.31"
    application
}

group = "io.github.gquintana"
version = "1.0-SNAPSHOT"

repositories {
    jcenter()
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/kotlinx-html/maven") }
}

val ktor_version = "1.6.0"
dependencies {
    testImplementation(kotlin("test"))
	testImplementation("com.salesforce.kafka.test:kafka-junit5:3.2.3")
	testImplementation("org.apache.kafka:kafka_2.13:3.0.0")
	testImplementation("io.ktor:ktor-client-core:$ktor_version")
	testImplementation("io.ktor:ktor-client-serialization:$ktor_version")
	testImplementation("io.ktor:ktor-client-apache:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-html-builder:$ktor_version")
	implementation("io.ktor:ktor-serialization:$ktor_version")
	implementation("org.jetbrains.kotlinx:kotlinx-html-jvm:0.7.2")
	implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.2")
	implementation("org.apache.kafka:kafka-clients:3.0.0")
	runtimeOnly("ch.qos.logback:logback-classic:1.2.10")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

application {
    mainClass.set("ServerKt")
}
