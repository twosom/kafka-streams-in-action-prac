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
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
