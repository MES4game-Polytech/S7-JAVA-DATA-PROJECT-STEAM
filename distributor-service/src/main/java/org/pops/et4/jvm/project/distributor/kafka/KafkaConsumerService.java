package org.pops.et4.jvm.project.distributor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.pops.et4.jvm.project.distributor.db.DistributorDbConfig;
import org.pops.et4.jvm.project.schemas.events.ConsumeLog;
import org.pops.et4.jvm.project.schemas.events.ExampleEvent;
import org.pops.et4.jvm.project.schemas.events.KafkaEvent;
import org.pops.et4.jvm.project.schemas.repositories.distributor.*;
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

    public static final String BEAN_NAME = "distributorServiceKafkaConsumerService";

    public static final String EXAMPLE_EVENT_CONSUMER_BEAN_NAME = "distributorServiceExampleEventConsumer";

    private final KafkaProducerService producerService;
    private final DistributorRepository distributorRepository;
    private final DistributedGameRepository distributedGameRepository;
    private final PlayerRepository playerRepository;
    private final OwnedGameRepository ownedGameRepository;
    private final ReviewRepository reviewRepository;

    private final List<ConsumeLog<? extends KafkaEvent>> logs = new ArrayList<>();

    @Autowired
    public KafkaConsumerService(
            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producerService,
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository,
            @Qualifier(DistributedGameRepository.BEAN_NAME) DistributedGameRepository distributedGameRepository,
            @Qualifier(PlayerRepository.BEAN_NAME) PlayerRepository playerRepository,
            @Qualifier(OwnedGameRepository.BEAN_NAME) OwnedGameRepository ownedGameRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository
    ) {
        this.producerService = producerService;
        this.distributorRepository = distributorRepository;
        this.distributedGameRepository = distributedGameRepository;
        this.playerRepository = playerRepository;
        this.ownedGameRepository = ownedGameRepository;
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
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
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
        //Distributor entity = Distributor.newBuilder()
        //        .setId(null)
        //        .setName(event.getPayload())
        //        .setIsCompany(true)
        //        .build();
        //Distributor savedDistributor = this.distributorRepository.save(entity);

        // To call a producer if needed:
        //this.producerService.sendExampleEvent();

        System.out.println("[Consumer] " + ExampleEvent.TOPIC + "(" + record.key() + "): FINISHED");
    }

}