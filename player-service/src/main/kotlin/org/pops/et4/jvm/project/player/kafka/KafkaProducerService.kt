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
    private val installedGameRepository: InstalledGameRepository,
    @Qualifier(org.pops.et4.jvm.project.schemas.repositories.publisher.GameRepository.BEAN_NAME)
    private val gameRepository: org.pops.et4.jvm.project.schemas.repositories.publisher.GameRepository
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
        
        /**
         * Compares two version strings (e.g., "1.2.3" vs "1.3.0")
         * Returns: positive if v1 > v2, negative if v1 < v2, zero if equal
         */
        fun compareVersions(v1: String, v2: String): Int {
            val parts1 = v1.split(".").map { it.toIntOrNull() ?: 0 }
            val parts2 = v2.split(".").map { it.toIntOrNull() ?: 0 }
            val maxLength = maxOf(parts1.size, parts2.size)
            
            for (i in 0 until maxLength) {
                val p1 = parts1.getOrNull(i) ?: 0
                val p2 = parts2.getOrNull(i) ?: 0
                val diff = p1 - p2
                if (diff != 0) return diff
            }
            return 0
        }
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
        
        // Get game info from publisher database
        val game = gameRepository.findById(gameId).orElse(null)
        if (game == null) {
            println("[Error] Game not found: gameId=${gameId}")
            return
        }
        
        val gameVersion = game.version ?: "1.0.0"
        
        // Check if game is already installed
        val existingGames = installedGameRepository.findAll()
            .filter { it.playerId == playerId && it.gameId == gameId && it.platform?.toString() == platform }
        
        if (existingGames.isNotEmpty()) {
            println("[Error] Game already installed: Player=${playerId}, Game=${gameId}, Platform=${platform}")
            println("        Current version: ${existingGames.first().installedVersion}")
            println("        Use 'update' command to update the version instead.")
            return
        }
        
        // Update database: Add to installed games with actual game version
        val installedGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
            .setId(null)
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(org.pops.et4.jvm.project.schemas.models.player.Platform.valueOf(platform))
            .setInstalledVersion(gameVersion)
            .build()
        
        val saved = installedGameRepository.save(installedGame)
        println("[Database] Installed game added: ID=${saved.id}, Player=${playerId}, Game=${gameId}, Platform=${platform}, Version=${gameVersion}")
        
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
        
        // Get game info from publisher database
        val game = gameRepository.findById(gameId).orElse(null)
        if (game == null) {
            println("[Error] Game not found: gameId=${gameId}")
            return
        }
        
        val newGameVersion = game.version ?: "1.0.0"
        
        // Update database: Update installed game version
        try {
            val installedGames = installedGameRepository.findAll()
                .filter { it.playerId == playerId && it.gameId == gameId && it.platform?.toString() == platform }
            
            if (installedGames.isEmpty()) {
                println("[Error] Game not installed. Cannot update.")
                println("        Use 'install' command to install the game first.")
                return
            }
            
            val installedGame = installedGames.first()
            val currentVersion = installedGame.installedVersion ?: "1.0.0"
            
            // Check if new version is higher than current version
            val versionComparison = compareVersions(newGameVersion, currentVersion)
            if (versionComparison <= 0) {
                println("[Error] Cannot update game. New version ($newGameVersion) is not higher than current version ($currentVersion).")
                println("        Current version: $currentVersion")
                println("        Available version: $newGameVersion")
                return
            }
            
            // Proceed with update
            val updatedGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
                .setId(installedGame.id)
                .setPlayerId(installedGame.playerId)
                .setGameId(installedGame.gameId)
                .setPlatform(installedGame.platform)
                .setInstalledVersion(newGameVersion)
                .build()
            
            val saved = installedGameRepository.save(updatedGame)
            println("[Database] Game updated: ID=${saved.id}, Player=${playerId}, Game=${gameId}, Platform=${platform}")
            println("            Old Version: ${currentVersion} → New Version: ${newGameVersion}")
        } catch (e: Exception) {
            println("[Database] Error updating game: ${e.message}")
            return
        }
        
        // Send Kafka event
        val topic = UpdateGame.TOPIC
        val key = UUID.randomUUID().toString()
        val event = UpdateGame.newBuilder()
            .setPlayerId(playerId)
            .setGameId(gameId)
            .setPlatform(platform)
            .setInstalledVersion(newGameVersion)
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