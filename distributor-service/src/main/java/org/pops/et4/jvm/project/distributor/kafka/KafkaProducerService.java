package org.pops.et4.jvm.project.distributor.kafka;

import org.pops.et4.jvm.project.schemas.events.ExampleEvent;
import org.pops.et4.jvm.project.schemas.repositories.distributor.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service(KafkaProducerService.BEAN_NAME)
public class KafkaProducerService {

    public static final String BEAN_NAME = "distributorServiceKafkaProducerService";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final DistributorRepository distributorRepository;
    private final DistributedGameRepository distributedGameRepository;
    private final PlayerRepository playerRepository;
    private final OwnedGameRepository ownedGameRepository;
    private final ReviewRepository reviewRepository;

    @Autowired
    public KafkaProducerService(
            @Qualifier(KafkaConfig.KAFKA_TEMPLATE_BEAN_NAME) KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository,
            @Qualifier(DistributedGameRepository.BEAN_NAME) DistributedGameRepository distributedGameRepository,
            @Qualifier(PlayerRepository.BEAN_NAME) PlayerRepository playerRepository,
            @Qualifier(OwnedGameRepository.BEAN_NAME) OwnedGameRepository ownedGameRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.distributorRepository = distributorRepository;
        this.distributedGameRepository = distributedGameRepository;
        this.playerRepository = playerRepository;
        this.ownedGameRepository = ownedGameRepository;
        this.reviewRepository = reviewRepository;
    }

    public void sendExampleEvent(String payload) {
        String topic = ExampleEvent.TOPIC;
        String key = UUID.randomUUID().toString();
        ExampleEvent event = ExampleEvent.newBuilder()
                .setPayload(payload)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

}