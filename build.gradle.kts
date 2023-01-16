plugins {
    id("java")
}

group = "com.icloud"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.3.1")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("com.github.javafaker:javafaker:0.12") {
        exclude("ch.qos.logback")
        exclude("org.slf4j", "slf4j-log4j12")
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
