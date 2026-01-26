package org.pops.et4.jvm.project.player.kafka

import org.pops.et4.jvm.project.schemas.events.*
import org.pops.et4.jvm.project.schemas.repositories.player.InstalledGameRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.Instant
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
        
        // Valid platform values
        val VALID_PLATFORMS = setOf(
            "WINDOWS", "MACOS", "LINUX", 
            "PS5", "PS4", 
            "XBOX_SERIES", "XBOX_ONE", 
            "SWITCH2", "SWITCH"
        )
    }


    fun sendRegisterPlayer(
        distributorId: Long,
        pseudo: String,
        firstName: String,
        lastName: String,
        birthDate: Instant
    ) {
        val topic = RegisterPlayer.TOPIC
        val key = UUID.randomUUID().toString()
        val event = RegisterPlayer.newBuilder()
            .setDistributorId(distributorId)
            .setPseudo(pseudo)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(birthDate)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendPurchaseGame(playerId: Long, gameId: Long) {
        val topic = PurchaseGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = PurchaseGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendReviewGame(playerId: Long, gameId: Long, rating: Int, comment: String?) {
        // Validate rating (0-5)
        if (rating !in 0..5) {
            println("[Error] Invalid rating: $rating. Rating must be between 0 and 5.")
            return
        }
        
        val topic = ReviewGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = ReviewGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setRating(rating)
            .setComment(comment)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendInstallGame(playerId: Long, gameId: Long, platform: String) {
        // Validate platform
        if (platform !in VALID_PLATFORMS) {
            println("[Error] Invalid platform: $platform. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
            return
        }
        
        // Send Kafka event
        val topic = InstallGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = InstallGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(platform)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendUpdateGame(playerId: Long, gameId: Long, platform: String, installedVersion: String) {
        // Validate platform
        if (platform !in VALID_PLATFORMS) {
            println("[Error] Invalid platform: $platform. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
            return
        }
        
        // Get installed game version from database
        val installedGames = installedGameRepository.findAll()
            .filter { it.playerId == playerId && it.gameId == gameId && it.platform?.toString() == platform }
        
        if (installedGames.isEmpty()) {
            println("[Error] Game not installed. Cannot update.")
            println("        Use 'install' command to install the game first.")
            return
        }
        
        val currentInstalledVersion = installedGames.first().installedVersion ?: "1.0.0"
        
        // Send Kafka event with the installed version
        val topic = UpdateGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = UpdateGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(platform)
            .setInstalledVersion(currentInstalledVersion)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendUninstallGame(playerId: Long, gameId: Long, platform: String, comment: String?) {
        // Validate platform
        if (platform !in VALID_PLATFORMS) {
            println("[Error] Invalid platform: $platform. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
            return
        }
        
        // Update database: Remove from installed games
        try {
            val installedGames = installedGameRepository.findAll()
                .filter { it.playerId == playerId && it.gameId == gameId && it.platform?.toString() == platform }
            
            if (installedGames.isNotEmpty()) {
                installedGameRepository.deleteAll(installedGames)
                println("[Database] Uninstalled game: Player=${playerId}, Game=${gameId}, Platform=${platform}")
            } else {
                println("[Database] Warning: Game not found in installed games")
            }
        } catch (e: Exception) {
            println("[Database] Error uninstalling game: ${e.message}")
        }
        
        // Send Kafka event
        val topic = UninstallGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = UninstallGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(platform)
            .setComment(comment)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendAddPlayTime(playerId: Long, gameId: Long, time: Long) {
        val topic = AddPlayTime.TOPIC
        val key = UUID.randomUUID().toString()
        val event = AddPlayTime.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setTime(time)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendReportCrash(
        playerId: Long,
        gameId: Long,
        platform: String,
        installedVersion: String,
        errorCode: Long,
        message: String
    ) {
        // Validate platform
        if (platform !in VALID_PLATFORMS) {
            println("[Error] Invalid platform: $platform. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
            return
        }
        
        val topic = ReportCrash.TOPIC
        val key = UUID.randomUUID().toString()
        val event = ReportCrash.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(platform)
            .setInstalledVersion(installedVersion)
            .setErrorCode(errorCode)
            .setMessage(message)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val msg = ex?.message ?: result
            println("[Producer] $topic($key): $msg")
        }
    }

    fun sendAddWishedGame(playerId: Long, gameId: Long) {
        val topic = AddWishedGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = AddWishedGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendRemoveWishedGame(playerId: Long, gameId: Long) {
        val topic = RemoveWishedGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = RemoveWishedGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }

    fun sendReactReview(playerId: Long, reviewId: Long, reactType: Int) {
        // Validate reactType (0-2: 0=NOTHING, 1=POSITIVE, 2=NEGATIVE)
        if (reactType !in 0..2) {
            println("[Error] Invalid reactType: $reactType. ReactType must be 0=NOTHING, 1=POSITIVE, or 2=NEGATIVE.")
            return
        }
        
        val topic = ReactReview.TOPIC
        val key = UUID.randomUUID().toString()
        val event = ReactReview.newBuilder()
            .setPlayerId(playerId)
            .setReviewId(reviewId)
            .setReactType(reactType)
            .build()

        val future = kafkaTemplate.send(topic, key, event)

        future.whenComplete { result, ex ->
            val message = ex?.message ?: result
            println("[Producer] $topic($key): $message")
        }
    }
}