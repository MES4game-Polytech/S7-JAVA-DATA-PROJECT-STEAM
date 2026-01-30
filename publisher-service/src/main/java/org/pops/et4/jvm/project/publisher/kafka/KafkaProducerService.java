package org.pops.et4.jvm.project.publisher.kafka;

import org.pops.et4.jvm.project.schemas.events.ExampleEvent;
import org.pops.et4.jvm.project.schemas.events.GamePublished;
import org.pops.et4.jvm.project.schemas.events.PatchPublished;
import org.pops.et4.jvm.project.schemas.models.publisher.Game;
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

    public void sendGamePublished(Game game){
        String topic = GamePublished.TOPIC;
        String key = UUID.randomUUID().toString();

        // Conversion des List<Enum> en List<String>
        List<String> platformStrings = game.getPlatforms().stream()
                .map(Enum::name)
                .toList();

        List<String> genreStrings = game.getGenres().stream()
                .map(Enum::name)
                .toList();

        GamePublished event = GamePublished.newBuilder()
                .setGameId(game.getId())
                .setGameName(game.getName())
                .setVersion(game.getVersion())
                .setPublisherId(game.getPublisher().getId())
                .setPlatforms(platformStrings)
                .setGenres(genreStrings)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) ->
                System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendPatchPublished(long gameId, String version){
        String topic = PatchPublished.TOPIC;
        String key = UUID.randomUUID().toString();
        PatchPublished event = PatchPublished.newBuilder()
                .setGameId(gameId)
                .setVersion(version)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) ->
                System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

}