package org.pops.et4.jvm.project.publisher.kafka;

import org.pops.et4.jvm.project.schemas.events.ExampleEvent;
import org.pops.et4.jvm.project.schemas.events.*;
import org.pops.et4.jvm.project.schemas.models.publisher.Genre;
import org.pops.et4.jvm.project.schemas.models.publisher.LogTag;
import org.pops.et4.jvm.project.schemas.models.publisher.Platform;
import org.pops.et4.jvm.project.schemas.repositories.publisher.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service(KafkaProducerService.BEAN_NAME)
public class KafkaProducerService {

    public static final String BEAN_NAME = "publisherServiceKafkaProducerService";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PublisherRepository publisherRepository;
    private final GameRepository gameRepository;
    private final PatchRepository patchRepository;
    private final CrashReportRepository crashReportRepository;
    private final ReviewRepository reviewRepository;

    @Autowired
    public KafkaProducerService(
            @Qualifier(KafkaConfig.KAFKA_TEMPLATE_BEAN_NAME) KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier(PublisherRepository.BEAN_NAME) PublisherRepository publisherRepository,
            @Qualifier(GameRepository.BEAN_NAME) GameRepository gameRepository,
            @Qualifier(PatchRepository.BEAN_NAME) PatchRepository patchRepository,
            @Qualifier(CrashReportRepository.BEAN_NAME) CrashReportRepository crashReportRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.publisherRepository = publisherRepository;
        this.gameRepository = gameRepository;
        this.patchRepository = patchRepository;
        this.crashReportRepository = crashReportRepository;
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

    public void sendGamePublished(long gameId){
        String topic = GamePublished.TOPIC;
        String key = UUID.randomUUID().toString();
        GamePublished event = GamePublished.newBuilder()
                .setGameId(gameId)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) ->
                System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendPatchPublished(Long patchId){
        String topic = PatchPublished.TOPIC;
        String key = UUID.randomUUID().toString();
        PatchPublished event = PatchPublished.newBuilder()
                .setPatchId(patchId)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) ->
                System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

}