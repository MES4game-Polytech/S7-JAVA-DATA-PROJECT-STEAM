import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `java-library`
    kotlin("jvm")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    kotlin("plugin.spring")
    kotlin("plugin.jpa")
}

dependencies {
    api(project(":schemas"))
    api("org.jetbrains.kotlin:kotlin-reflect:2.3.0")
    api("org.jetbrains.kotlin:kotlin-stdlib:2.3.0")
    api("org.springframework.boot:spring-boot-starter")
    api("org.springframework.boot:spring-boot-starter-web")
    api("org.springframework.boot:spring-boot-starter-data-jpa")
    api("org.springframework.kafka:spring-kafka")
    api("io.confluent:kafka-avro-serializer:8.1.1")
    runtimeOnly("org.postgresql:postgresql:42.7.8")

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.getByName<JavaExec>("bootRun") {
    standardInput = System.`in`
}

tasks.named<Jar>("jar") {
    enabled = true
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveClassifier.set("boot")
}

tasks.test {
    useJUnitPlatform()
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.compilerOptions {
    freeCompilerArgs.set(listOf("-Xannotation-default-target=param-property"))
}