package org.pops.et4.jvm.project.distributor.kafka;

import org.pops.et4.jvm.project.schemas.events.*;
import org.pops.et4.jvm.project.schemas.repositories.distributor.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
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

    public void sendGameDistributed(Long distributorId, Long gameId, String gameName) {
        String topic = GameDistributed.TOPIC;
        String key = UUID.randomUUID().toString();
        GameDistributed event = GameDistributed.newBuilder()
                .setDistributorId(distributorId)
                .setGameId(gameId)
                .setGameName(gameName)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendPatchDistributed(Long distributorId, Long gameId, String newVersion, String gameName) {
        String topic = PatchDistributed.TOPIC;
        String key = UUID.randomUUID().toString();
        PatchDistributed event = PatchDistributed.newBuilder()
                .setDistributorId(distributorId)
                .setGameId(gameId)
                .setNewVersion(newVersion)
                .setGameName(gameName)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendSaleStarted(Long distributorId, Long gameId, Float salePercentage, String gameName) {
        String topic = SaleStarted.TOPIC;
        String key = UUID.randomUUID().toString();
        SaleStarted event = SaleStarted.newBuilder()
                .setDistributorId(distributorId)
                .setGameId(gameId)
                .setSalePercentage(salePercentage)
                .setGameName(gameName)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendSendGameFile(Long targetId, Long gameId, String version, String gameName, String platform, String playerName) {
        String topic = SendGameFile.TOPIC;
        String key = UUID.randomUUID().toString();
        SendGameFile event = SendGameFile.newBuilder()
                .setTargetId(targetId)
                .setGameId(gameId)
                .setVersion(version)
                .setGameName(gameName)
                .setPlatform(platform)
                .setPlayerName(playerName)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendGameReviewed(Long reviewId, Long distributorId, Integer rating, String comment, Instant publicationDate, java.util.List<Long> positiveReactionPlayerIds, java.util.List<Long> negativeReactionPlayerIds) {
        String topic = GameReviewed.TOPIC;
        String key = UUID.randomUUID().toString();
        GameReviewed event = GameReviewed.newBuilder()
                .setReviewId(reviewId)
                .setDistributorId(distributorId)
                .setRating(rating)
                .setComment(comment)
                .setPublicationDate(publicationDate)
                .setPositiveReactionPlayerIds(positiveReactionPlayerIds)
                .setNegativeReactionPlayerIds(negativeReactionPlayerIds)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendReviewRefused(Long reviewId, String playerName, String gameName) {
        String topic = ReviewRefused.TOPIC;
        String key = UUID.randomUUID().toString();
        ReviewRefused event = ReviewRefused.newBuilder()
                .setReviewId(reviewId)
                .setPlayerName(playerName)
                .setGameName(gameName)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendCrashReported(Long distributorId, Long gameId, Platform platform, String installedVersion, Integer errorCode, String message) {
        String topic = CrashReported.TOPIC;
        String key = UUID.randomUUID().toString();
        CrashReported event = CrashReported.newBuilder()
                .setDistributorId(distributorId)
                .setGameId(gameId)
                .setPlatform(platform)
                .setInstalledVersion(installedVersion)
                .setErrorCode(errorCode)
                .setMessage(message)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

    public void sendSendPlayerPage(String page) {
        String topic = SendPlayerPage.TOPIC;
        String key = UUID.randomUUID().toString();
        SendPlayerPage event = SendPlayerPage.newBuilder()
                .setPage(page)
                .build();

        CompletableFuture<?> future = this.kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> System.out.println("[Producer] " + topic + "(" + key + "): " + (ex==null ? result : ex.getMessage())));
    }

}