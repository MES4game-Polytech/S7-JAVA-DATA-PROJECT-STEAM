package org.pops.et4.jvm.project.player.kafka

import org.pops.et4.jvm.project.schemas.events.ExampleEvent
import org.pops.et4.jvm.project.schemas.repositories.player.InstalledGameRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID

@Service(KafkaProducerService.BEAN_NAME)
class KafkaProducerService(
    @Qualifier(KafkaConfig.KAFKA_TEMPLATE_BEAN_NAME)
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    @Qualifier(InstalledGameRepository.BEAN_NAME)
    private val installedGameRepository: InstalledGameRepository
) {
	companion object {
        const val BEAN_NAME = "playerServiceKafkaProducerService"
    }

    fun sendExampleEvent(payload: String) {
        val topic = ExampleEvent.TOPIC
        val key = UUID.randomUUID().toString()
        val event = ExampleEvent.newBuilder()
            .setPayload(payload)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }
}