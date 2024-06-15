import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.10"
    application
}
//Probabilistic Data Structures
//Bloom filters, MinHash, Count-min sketch, HyperLogLog
group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")

//    implementation("com.clearspring.analytics:stream:2.9.8")
    implementation("com.google.guava:guava:33.2.1-jre")

}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

application {
    mainClass.set("MainKt")
}