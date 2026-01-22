package org.pops.et4.jvm.project.player.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.pops.et4.jvm.project.player.db.PlayerDbConfig
import org.pops.et4.jvm.project.schemas.events.ConsumeLog
import org.pops.et4.jvm.project.schemas.events.ExampleEvent
import org.pops.et4.jvm.project.schemas.events.KafkaEvent
import org.pops.et4.jvm.project.schemas.repositories.player.InstalledGameRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.*

@Service(KafkaConsumerService.BEAN_NAME)
class KafkaConsumerService(
	@Qualifier(KafkaProducerService.BEAN_NAME)
    private val producerService: KafkaProducerService,
    @Qualifier(InstalledGameRepository.BEAN_NAME) 
    private val installedGameRepository: InstalledGameRepository
) {

    companion object {
        const val BEAN_NAME = "playerServiceKafkaConsumerService"

        const val EXAMPLE_EVENT_CONSUMER_BEAN_NAME = "playerServiceExampleEventConsumer"
    }

    private val _logs = ArrayList<ConsumeLog<out KafkaEvent>>()

    val logs: List<ConsumeLog<out KafkaEvent>>
        get() = Collections.unmodifiableList(_logs)

    @KafkaListener(
        id = EXAMPLE_EVENT_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [ExampleEvent.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeExampleEvent(record: ConsumerRecord<String, ExampleEvent>) {
        _logs.add(
            ConsumeLog(
                EXAMPLE_EVENT_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()

        // To add a new element in the database:
        // val entity = Player.newBuilder()
        //        .setGameId(1L)
        //        .setDistributorId(1L)
        //        .setName(event.getPayload())
        //        .setIsCompany(true)
        //        .build()
        // val savedInstalledGame = this.installedGameRepository.save(entity)

        // To call a producer if needed:
        // this.producerService.sendExampleEvent()

        println("[Consumer] ${ExampleEvent.TOPIC}(${record.key()}): FINISHED")
    }
}