package org.pops.et4.jvm.project.distributor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.pops.et4.jvm.project.distributor.DistributorService;
import org.pops.et4.jvm.project.distributor.db.DistributorDbConfig;
import org.pops.et4.jvm.project.schemas.events.*;
import org.pops.et4.jvm.project.schemas.models.distributor.DistributedGame;
import org.pops.et4.jvm.project.schemas.models.distributor.Review;
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
    public static final String GAME_PUBLISHED_CONSUMER_BEAN_NAME = "distributorServiceGamePublishedConsumer";
    public static final String PATCH_PUBLISHED_CONSUMER_BEAN_NAME = "distributorServicePatchPublishedConsumer";
    public static final String REGISTER_PLAYER_CONSUMER_BEAN_NAME = "distributorServiceRegisterPlayerConsumer";
    public static final String PURCHASE_GAME_CONSUMER_BEAN_NAME = "distributorServicePurchaseGameConsumer";
    public static final String REVIEW_GAME_CONSUMER_BEAN_NAME = "distributorServiceReviewGameConsumer";
    public static final String REACT_REVIEW_CONSUMER_BEAN_NAME = "distributorServiceReactReviewConsumer";
    public static final String INSTALL_GAME_CONSUMER_BEAN_NAME = "distributorServiceInstallGameConsumer";
    public static final String UPDATE_GAME_CONSUMER_BEAN_NAME = "distributorServiceUpdateGameConsumer";
    public static final String UNINSTALL_GAME_CONSUMER_BEAN_NAME = "distributorServiceUninstallGameConsumer";
    public static final String ADD_PLAY_TIME_CONSUMER_BEAN_NAME = "distributorServiceAddPlayTimeConsumer";
    public static final String REPORT_CRASH_CONSUMER_BEAN_NAME = "distributorServiceReportCrashConsumer";
    public static final String ADD_WISHED_GAME_CONSUMER_BEAN_NAME = "distributorServiceAddWishedGameConsumer";
    public static final String REMOVE_WISHED_GAME_CONSUMER_BEAN_NAME = "distributorServiceRemoveWishedGameConsumer";
    public static final String ASK_PLAYER_PAGE_CONSUMER_BEAN_NAME = "distributorServiceAskPlayerPageConsumer";

    private final KafkaProducerService producerService;
    private final DistributorService distributorService;
    private final DistributorRepository distributorRepository;
    private final DistributedGameRepository distributedGameRepository;
    private final PlayerRepository playerRepository;
    private final OwnedGameRepository ownedGameRepository;
    private final ReviewRepository reviewRepository;

    private final List<ConsumeLog<? extends KafkaEvent>> logs = new ArrayList<>();

    @Autowired
    public KafkaConsumerService(
            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producerService,
            @Qualifier(DistributorService.BEAN_NAME) DistributorService distributorService,
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository,
            @Qualifier(DistributedGameRepository.BEAN_NAME) DistributedGameRepository distributedGameRepository,
            @Qualifier(PlayerRepository.BEAN_NAME) PlayerRepository playerRepository,
            @Qualifier(OwnedGameRepository.BEAN_NAME) OwnedGameRepository ownedGameRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository
    ) {
        this.producerService = producerService;
        this.distributorService = distributorService;
        this.distributorRepository = distributorRepository;
        this.distributedGameRepository = distributedGameRepository;
        this.playerRepository = playerRepository;
        this.ownedGameRepository = ownedGameRepository;
        this.reviewRepository = reviewRepository;
    }

    public List<ConsumeLog<? extends KafkaEvent>> getLogs() {
        return Collections.unmodifiableList(this.logs);
    }

    /*
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
    */

    @KafkaListener(
            id = KafkaConsumerService.GAME_PUBLISHED_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = GamePublished.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeGamePublished(ConsumerRecord<String, GamePublished> record) {
        this.logs.add(new ConsumeLog<>("gamePublishedConsumer", Instant.now(), record.key(), record.value()));
        GamePublished event = record.value();

        // Business logic: Create DistributedGame and send game-distributed event
        var distributedGame = distributorService.gamePublished(event);
        String gameName = distributorService.getGameName(event.getGameId());
        producerService.sendGameDistributed(distributedGame.getDistributor().getId(), event.getGameId(), gameName);

        System.out.println("[Consumer] " + GamePublished.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.PATCH_PUBLISHED_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = PatchPublished.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumePatchPublished(ConsumerRecord<String, PatchPublished> record) {
        this.logs.add(new ConsumeLog<>("patchPublishedConsumer", Instant.now(), record.key(), record.value()));
        PatchPublished event = record.value();

        // Business logic: Update DistributedGame version and send patch-distributed event
        var distributedGame = distributorService.patchPublished(event);
        String gameName = distributorService.getGameName(event.getGameId());
        producerService.sendPatchDistributed(distributedGame.getDistributor().getId(), event.getGameId(), event.getVersion(), gameName);

        System.out.println("[Consumer] " + PatchPublished.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.REGISTER_PLAYER_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = RegisterPlayer.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeRegisterPlayer(ConsumerRecord<String, RegisterPlayer> record) {
        this.logs.add(new ConsumeLog<>("registerPlayerConsumer", Instant.now(), record.key(), record.value()));
        RegisterPlayer event = record.value();

        // Business logic: Register the player in distributor's database
        distributorService.registerPlayer(event);

        System.out.println("[Consumer] " + RegisterPlayer.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.PURCHASE_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = PurchaseGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumePurchaseGame(ConsumerRecord<String, PurchaseGame> record) {
        this.logs.add(new ConsumeLog<>("purchaseGameConsumer", Instant.now(), record.key(), record.value()));
        PurchaseGame event = record.value();

        // Business logic: Create OwnedGame entry when player purchases a game
        distributorService.purchaseGame(event);

        System.out.println("[Consumer] " + PurchaseGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.REVIEW_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = ReviewGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeReviewGame(ConsumerRecord<String, ReviewGame> record) {
        this.logs.add(new ConsumeLog<>("reviewGameConsumer", Instant.now(), record.key(), record.value()));
        ReviewGame event = record.value();

        // Business logic: Save review in database and send game-reviewed event
        // If review is refused (insufficient playtime), send review-refused event instead
        try {
            Review savedReview = distributorService.reviewGame(event);
            producerService.sendGameReviewed(savedReview.getId());
        } catch (IllegalStateException e) {
            // Review refused - send ReviewRefused event
            // We need to create a review with ID first to get the reviewId
            // Or we can send a mock ID, depending on the use case
            String playerName = distributorService.getPlayerName(event.getPlayerId());
            String gameName = distributorService.getGameName(event.getGameId());
            producerService.sendReviewRefused(0L, playerName, gameName); // Using 0 as placeholder since review wasn't created
            System.out.println("[Consumer] " + ReviewGame.TOPIC + "(" + record.key() + "): REFUSED - " + e.getMessage());
        }

        System.out.println("[Consumer] " + ReviewGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.REACT_REVIEW_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = ReactReview.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeReactReview(ConsumerRecord<String, ReactReview> record) {
        this.logs.add(new ConsumeLog<>("reactReviewConsumer", Instant.now(), record.key(), record.value()));
        ReactReview event = record.value();

        // Business logic: Add or remove reaction to a review
        distributorService.reactReview(event);

        System.out.println("[Consumer] " + ReactReview.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.INSTALL_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = InstallGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeInstallGame(ConsumerRecord<String, InstallGame> record) {
        this.logs.add(new ConsumeLog<>("installGameConsumer", Instant.now(), record.key(), record.value()));
        InstallGame event = record.value();

        // Business logic: Verify player owns game, then send game files
        try {
            DistributedGame game = distributorService.processInstallGame(event.getPlayerId(), event.getGameId());
            String gameName = distributorService.getGameName(event.getGameId());
            String playerName = distributorService.getPlayerName(event.getPlayerId());
            String platform = event.getPlatform().toString();
            producerService.sendSendGameFile(event.getPlayerId(), event.getGameId(), game.getVersion(), gameName, platform, playerName);
        } catch (IllegalStateException e) {
            System.out.println("[Consumer] " + InstallGame.TOPIC + "(" + record.key() + "): REFUSED - " + e.getMessage());
        }

        System.out.println("[Consumer] " + InstallGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.UPDATE_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = UpdateGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeUpdateGame(ConsumerRecord<String, UpdateGame> record) {
        this.logs.add(new ConsumeLog<>("updateGameConsumer", Instant.now(), record.key(), record.value()));
        UpdateGame event = record.value();

        // Business logic: Verify player owns game and check if update is needed, then send updated game files
        try {
            DistributedGame game = distributorService.processUpdateGame(event.getPlayerId(), event.getGameId(), event.getInstalledVersion().toString());
            String gameName = distributorService.getGameName(event.getGameId());
            String playerName = distributorService.getPlayerName(event.getPlayerId());
            String platform = event.getPlatform().toString();
            producerService.sendSendGameFile(event.getPlayerId(), event.getGameId(), game.getVersion(), gameName, platform, playerName);
        } catch (IllegalStateException e) {
            System.out.println("[Consumer] " + UpdateGame.TOPIC + "(" + record.key() + "): REFUSED - " + e.getMessage());
        }

        System.out.println("[Consumer] " + UpdateGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.UNINSTALL_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = UninstallGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeUninstallGame(ConsumerRecord<String, UninstallGame> record) {
        this.logs.add(new ConsumeLog<>("uninstallGameConsumer", Instant.now(), record.key(), record.value()));
        UninstallGame event = record.value();

        // Log the uninstallation event for tracking purposes
        System.out.println("[Consumer] Player " + event.getPlayerId() + " uninstalled game " + event.getGameId() + " from platform " + event.getPlatform());

        System.out.println("[Consumer] " + UninstallGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.ADD_PLAY_TIME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = AddPlayTime.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeAddPlayTime(ConsumerRecord<String, AddPlayTime> record) {
        this.logs.add(new ConsumeLog<>("addPlayTimeConsumer", Instant.now(), record.key(), record.value()));
        AddPlayTime event = record.value();

        // Business logic: Update playtime in OwnedGame
        distributorService.addPlayTime(event);

        System.out.println("[Consumer] " + AddPlayTime.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.REPORT_CRASH_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = ReportCrash.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeReportCrash(ConsumerRecord<String, ReportCrash> record) {
        this.logs.add(new ConsumeLog<>("reportCrashConsumer", Instant.now(), record.key(), record.value()));
        ReportCrash event = record.value();

        // Business logic: Process crash report and notify via crash-reported event
        Long distributorId = distributorService.processCrashReport(event);

        // Convert platform string to Platform enum
        org.pops.et4.jvm.project.schemas.events.Platform platformEnum =
                org.pops.et4.jvm.project.schemas.events.Platform.valueOf(event.getPlatform().toString());

        producerService.sendCrashReported(
                distributorId,
                event.getGameId(),
                platformEnum,
                event.getInstalledVersion().toString(),
                (int) event.getErrorCode(),
                event.getMessage().toString()
        );

        System.out.println("[Consumer] " + ReportCrash.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.ADD_WISHED_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = AddWishedGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeAddWishedGame(ConsumerRecord<String, AddWishedGame> record) {
        this.logs.add(new ConsumeLog<>("addWishedGameConsumer", Instant.now(), record.key(), record.value()));
        AddWishedGame event = record.value();

        // Business logic: Add game to player's wishlist
        distributorService.addWishedGame(event);

        System.out.println("[Consumer] " + AddWishedGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.REMOVE_WISHED_GAME_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = RemoveWishedGame.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeRemoveWishedGame(ConsumerRecord<String, RemoveWishedGame> record) {
        this.logs.add(new ConsumeLog<>("removeWishedGameConsumer", Instant.now(), record.key(), record.value()));
        RemoveWishedGame event = record.value();

        // Business logic: Remove game from player's wishlist
        distributorService.removeWishedGame(event);

        System.out.println("[Consumer] " + RemoveWishedGame.TOPIC + "(" + record.key() + "): FINISHED");
    }

    @KafkaListener(
            id = KafkaConsumerService.ASK_PLAYER_PAGE_CONSUMER_BEAN_NAME,
            containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
            topics = AskPlayerPage.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    @Transactional(transactionManager = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public void consumeAskPlayerPage(ConsumerRecord<String, AskPlayerPage> record) {
        this.logs.add(new ConsumeLog<>("askPlayerPageConsumer", Instant.now(), record.key(), record.value()));
        AskPlayerPage event = record.value();

        // Business logic: Generate and send player page
        String playerPage = distributorService.generatePlayerPage(event.getDistributorId());
        producerService.sendSendPlayerPage(playerPage);

        System.out.println("[Consumer] " + AskPlayerPage.TOPIC + "(" + record.key() + "): FINISHED");
    }
}

