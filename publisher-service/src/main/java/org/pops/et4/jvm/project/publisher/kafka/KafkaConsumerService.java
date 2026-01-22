package org.pops.et4.jvm.project.publisher.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.pops.et4.jvm.project.publisher.db.PublisherDbConfig;
import org.pops.et4.jvm.project.schemas.events.ConsumeLog;
import org.pops.et4.jvm.project.schemas.events.ExampleEvent;
import org.pops.et4.jvm.project.schemas.events.KafkaEvent;
import org.pops.et4.jvm.project.schemas.repositories.publisher.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service(KafkaConsumerService.BEAN_NAME)
public class KafkaConsumerService {

    public static final String BEAN_NAME = "publisherServiceKafkaConsumerService";

    public static final String EXAMPLE_EVENT_CONSUMER_BEAN_NAME = "publisherServiceExampleEventConsumer";

    private final KafkaProducerService producerService;
    private final PublisherRepository publisherRepository;
    private final GameRepository gameRepository;
    private final PatchRepository patchRepository;
    private final CrashReportRepository crashReportRepository;
    private final ReviewRepository reviewRepository;

    private final List<ConsumeLog<? extends KafkaEvent>> logs = new ArrayList<>();

    @Autowired
    public KafkaConsumerService(
            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producerService,
            @Qualifier(PublisherRepository.BEAN_NAME) PublisherRepository publisherRepository,
            @Qualifier(GameRepository.BEAN_NAME) GameRepository gameRepository,
            @Qualifier(PatchRepository.BEAN_NAME) PatchRepository patchRepository,
            @Qualifier(CrashReportRepository.BEAN_NAME) CrashReportRepository crashReportRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository
    ) {
        this.producerService = producerService;
        this.publisherRepository = publisherRepository;
        this.gameRepository = gameRepository;
        this.patchRepository = patchRepository;
        this.crashReportRepository = crashReportRepository;
        this.reviewRepository = reviewRepository;
    }

    public List<ConsumeLog<? extends KafkaEvent>> getLogs() {
        return Collections.unmodifiableList(this.logs);
    }

    @KafkaListener(
            id = KafkaConsumerService.EXAMPLE_EVENT_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = ExampleEvent.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}",
            autoStartup = "false"
    )
    @Transactional(transactionManager = PublisherDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeExampleEvent(ConsumerRecord<String, ExampleEvent> record) {
        this.logs.add(
                new ConsumeLog<>(
                        KafkaConsumerService.EXAMPLE_EVENT_CONSUMER_BEAN_NAME,
                        Instant.now(),
                        record.key(),
                        record.value()
                )
        );

        ExampleEvent event = record.value();

        // To add a new element in the database:
        //Publisher entity = Publisher.newBuilder()
        //        .setId(null)
        //        .setName(event.getPayload())
        //        .setIsCompany(true)
        //        .build();
        //Publisher savedPublisher = this.publisherRepository.save(entity);

        // To call a producer if needed:
        //this.producerService.sendExampleEvent();

        System.out.println("[Consumer] " + ExampleEvent.TOPIC + "(" + record.key() + "): FINISHED");
    }

}