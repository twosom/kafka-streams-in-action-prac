plugins {
    id("java")
}

group = "com.icloud"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.3.2")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.apache.logging.log4j:log4j-core:2.19.0")
    implementation("org.slf4j:slf4j-api:2.0.6")
    implementation("org.slf4j:slf4j-simple:2.0.0-alpha0")
    implementation("com.github.javafaker:javafaker:1.0.2") {
        exclude("ch.qos.logback")
        exclude("org.slf4j", "slf4j-log4j12")
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
