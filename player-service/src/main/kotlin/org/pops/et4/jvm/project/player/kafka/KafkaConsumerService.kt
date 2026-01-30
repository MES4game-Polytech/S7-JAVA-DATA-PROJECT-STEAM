package org.pops.et4.jvm.project.player.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.pops.et4.jvm.project.player.db.PlayerDbConfig
import org.pops.et4.jvm.project.schemas.events.*
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

        const val GAME_DISTRIBUTED_CONSUMER_BEAN_NAME = "playerServiceGameDistributedConsumer"
        const val PATCH_DISTRIBUTED_CONSUMER_BEAN_NAME = "playerServicePatchDistributedConsumer"
        const val SALE_STARTED_CONSUMER_BEAN_NAME = "playerServiceSaleStartedConsumer"
        const val SEND_GAME_FILE_CONSUMER_BEAN_NAME = "playerServiceSendGameFileConsumer"
        const val REVIEW_REFUSED_CONSUMER_BEAN_NAME = "playerServiceReviewRefusedConsumer"
        const val SEND_PLAYER_PAGE_CONSUMER_BEAN_NAME = "playerServiceSendPlayerPageConsumer"
        const val SEND_GAMES_PAGE_CONSUMER_BEAN_NAME = "playerServiceSendGamesPageConsumer"
        const val SEND_GAME_REVIEWS_CONSUMER_BEAN_NAME = "playerServiceSendGameReviewsConsumer"
    }

    private val _logs = ArrayList<ConsumeLog<out KafkaEvent>>()

    val logs: List<ConsumeLog<out KafkaEvent>>
        get() = Collections.unmodifiableList(_logs)

    // ============================================
    // DISTRIBUTOR EVENT CONSUMERS
    // ============================================

    /**
     * Consumer for GameDistributed event
     * Triggered when a new game or DLC is added to the distributor's catalog
     */
    @KafkaListener(
        id = GAME_DISTRIBUTED_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [GameDistributed.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeGameDistributed(record: ConsumerRecord<String, GameDistributed>) {
        _logs.add(
            ConsumeLog(
                GAME_DISTRIBUTED_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        
        println("[Consumer] ${GameDistributed.TOPIC}(${record.key()}): New game '${event.getGameName()}' (ID: ${event.getGameId()}) available from distributor ${event.getDistributorId()}")
        println("[Consumer] ${GameDistributed.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for PatchDistributed event
     * Triggered when a game update/patch is available
     * Should notify players
     */
    @KafkaListener(
        id = PATCH_DISTRIBUTED_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [PatchDistributed.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumePatchDistributed(record: ConsumerRecord<String, PatchDistributed>) {
        _logs.add(
            ConsumeLog(
                PATCH_DISTRIBUTED_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        println("[Consumer] ${PatchDistributed.TOPIC}(${record.key()}): Patch ${event.getNewVersion()} available for '${event.getGameName()}' (ID: ${event.getGameId()})")
        
    }

    /**
     * Consumer for SaleStarted event
     * Triggered when a game goes on sale
     */
    @KafkaListener(
        id = SALE_STARTED_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [SaleStarted.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeSaleStarted(record: ConsumerRecord<String, SaleStarted>) {
        _logs.add(
            ConsumeLog(
                SALE_STARTED_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        val discountPercent = (event.getSalePercentage() * 100).toInt()
        
        println("[Consumer] ${SaleStarted.TOPIC}(${record.key()}): Sale ${discountPercent}% off on '${event.getGameName()}' (ID: ${event.getGameId()})")
        println("[Consumer] ${SaleStarted.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for SendGameFile event
     * Triggered when the distributor sends game files to update a player's game version
     * This happens when player's version doesn't match the latest
     * Automatically adds or updates the game in the InstalledGames table
     */
    @KafkaListener(
        id = SEND_GAME_FILE_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [SendGameFile.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeSendGameFile(record: ConsumerRecord<String, SendGameFile>) {
        _logs.add(
            ConsumeLog(
                SEND_GAME_FILE_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): Game files for '${event.getGameName()}' v${event.getVersion()} sent to player '${event.getPlayerName()}' (ID: ${event.getTargetId()}) on ${event.getPlatform()}")
        
        try {
            // Try to parse the platform enum
            val platformEnum = org.pops.et4.jvm.project.schemas.models.player.Platform.valueOf(event.getPlatform())
            
            // Find existing installation
            val existingGames = installedGameRepository.findAll()
                .filter { 
                    it.getPlayerId() == event.getTargetId() && 
                    it.getGameId() == event.getGameId() && 
                    it.getPlatform().toString() == event.getPlatform() 
                }
            
            if (existingGames.isNotEmpty()) {
                // UPDATE existing installation
                val existingGame = existingGames.first()
                val updatedGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
                    .setId(existingGame.getId())
                    .setPlayerId(existingGame.getPlayerId())
                    .setGameId(existingGame.getGameId())
                    .setPlatform(existingGame.getPlatform())
                    .setInstalledVersion(event.getVersion())
                    .build()
                
                val saved = installedGameRepository.save(updatedGame)
                println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): UPDATED installation ID=${saved.getId()}, Player=${event.getTargetId()}, Game=${event.getGameId()}, Platform=${event.getPlatform()}, Version=${event.getVersion()}")
            } else {
                // INSERT new installation
                val newGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
                    .setId(null)
                    .setPlayerId(event.getTargetId())
                    .setGameId(event.getGameId())
                    .setPlatform(platformEnum)
                    .setInstalledVersion(event.getVersion())
                    .build()
                
                val saved = installedGameRepository.save(newGame)
                println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): INSERTED new installation ID=${saved.getId()}, Player=${event.getTargetId()}, Game=${event.getGameId()}, Platform=${event.getPlatform()}, Version=${event.getVersion()}")
            }
        } catch (e: IllegalArgumentException) {
            System.err.println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): ERROR - Invalid platform '${event.getPlatform()}': ${e.message}")
        } catch (e: Exception) {
            System.err.println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): ERROR - ${e.message}")
            e.printStackTrace()
        }
        
        println("[Consumer] ${SendGameFile.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for ReviewRefused event
     * Triggered when a player's review has been refused by moderation
     */
    @KafkaListener(
        id = REVIEW_REFUSED_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [ReviewRefused.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeReviewRefused(record: ConsumerRecord<String, ReviewRefused>) {
        _logs.add(
            ConsumeLog(
                REVIEW_REFUSED_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        
        println("[Consumer] ${ReviewRefused.TOPIC}(${record.key()}): Review (ID: ${event.getReviewId()}) by '${event.getPlayerName()}' for '${event.getGameName()}' has been refused")
        println("[Consumer] ${ReviewRefused.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for SendPlayerPage event
     * Triggered when the distributor sends the player's profile page
     * Displays the formatted player page content
     */
    @KafkaListener(
        id = SEND_PLAYER_PAGE_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [SendPlayerPage.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeSendPlayerPage(record: ConsumerRecord<String, SendPlayerPage>) {
        _logs.add(
            ConsumeLog(
                SEND_PLAYER_PAGE_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        
        println("[Consumer] ${SendPlayerPage.TOPIC}(${record.key()}): Received player page:")
        println("=".repeat(80))
        println(event.getPage())
        println("=".repeat(80))
        println("[Consumer] ${SendPlayerPage.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for SendGamesPage event
     * Triggered when the distributor sends a page of available games
     * Displays the formatted games page content
     */
    @KafkaListener(
        id = SEND_GAMES_PAGE_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [SendGamesPage.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeSendGamesPage(record: ConsumerRecord<String, SendGamesPage>) {
        _logs.add(
            ConsumeLog(
                SEND_GAMES_PAGE_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        
        println("[Consumer] ${SendGamesPage.TOPIC}(${record.key()}): Received games page:")
        println("=".repeat(80))
        println(event.getPage())
        println("=".repeat(80))
        println("[Consumer] ${SendGamesPage.TOPIC}(${record.key()}): FINISHED")
    }

    /**
     * Consumer for SendGameReviews event
     * Triggered when the distributor sends reviews for a specific game
     * Displays the formatted game reviews content
     */
    @KafkaListener(
        id = SEND_GAME_REVIEWS_CONSUMER_BEAN_NAME,
        containerFactory = KafkaConfig.KAFKA_LISTENER_CONTAINER_BEAN_NAME,
        topics = [SendGameReviews.TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}",
        autoStartup = "false"
    )
    @Transactional(transactionManager = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    fun consumeSendGameReviews(record: ConsumerRecord<String, SendGameReviews>) {
        _logs.add(
            ConsumeLog(
                SEND_GAME_REVIEWS_CONSUMER_BEAN_NAME,
                Instant.now(),
                record.key(),
                record.value()
            )
        )

        val event = record.value()
        
        println("[Consumer] ${SendGameReviews.TOPIC}(${record.key()}): Received game reviews:")
        println("=".repeat(80))
        println(event.getPage())
        println("=".repeat(80))
        println("[Consumer] ${SendGameReviews.TOPIC}(${record.key()}): FINISHED")
    }
}