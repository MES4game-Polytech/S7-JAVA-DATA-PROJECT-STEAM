package org.pops.et4.jvm.project.player

import org.pops.et4.jvm.project.player.kafka.KafkaLifecycleService
import org.pops.et4.jvm.project.player.kafka.KafkaProducerService
import org.pops.et4.jvm.project.schemas.models.player.InstalledGame
import org.pops.et4.jvm.project.schemas.models.player.Platform
import org.pops.et4.jvm.project.schemas.repositories.player.InstalledGameRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import java.util.*

@SpringBootApplication
class App {

	companion object {
        const val CLI_BEAN_NAME = "playerServiceCLI"
        
        // Valid platform values
        val VALID_PLATFORMS = setOf(
            "WINDOWS", "MACOS", "LINUX", 
            "PS5", "PS4", 
            "XBOX_SERIES", "XBOX_ONE", 
            "SWITCH2", "SWITCH"
        )
    }
    
    // Timer state for playtime tracking
    private var timerStartTime: Long? = null
    private var timerPlayerId: Long? = null
    private var timerGameId: Long? = null

    @Bean(name = [App.CLI_BEAN_NAME])
    fun interactiveTestRunner(
        @Qualifier(KafkaProducerService.BEAN_NAME)producer: KafkaProducerService,
        @Qualifier(KafkaLifecycleService.BEAN_NAME)lifecycle: KafkaLifecycleService,
        @Qualifier(InstalledGameRepository.BEAN_NAME)installedGameRepository: InstalledGameRepository
    ): CommandLineRunner {
        return CommandLineRunner {
            Thread.sleep(1000)

            Scanner(System.`in`).use { scanner ->
                var running = true

                while (running) {
                    printMenu(timerStartTime != null)
                    if (!scanner.hasNextLine()) break
                    val input = scanner.nextLine().trim()
                    if (input.isEmpty()) continue

                    val cmd = input.split("\\s+".toRegex()).toTypedArray()
                    val args = cmd.sliceArray(1 until cmd.size)

                    println("---------------------------------")

                    when (cmd[0]) {
                        "exit", "quit" -> {
                            println("Exiting Manual Test Mode...")
                            running = false
                        }
                        "start" -> {
                            println("> Starting Listener(s)")
                            args.forEach { lifecycle.startListener(it) }
                        }
                        "stop" -> {
                            println("> Stopping Listener(s)")
                            args.forEach { lifecycle.stopListener(it) }
                        }

                        
                        // === PRODUCER COMMANDS ===
                        "register" -> {
                            println("> Registering Player...")
                            if (args.size < 5) {
                                System.err.println("Usage: register [distributorId] [pseudo] [firstName] [lastName] [birthDate(yyyy-MM-dd)]")
                            } else {
                                try {
                                    val birthDate = java.time.LocalDate.parse(args[4]).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant()
                                    producer.sendRegisterPlayer(args[0].toLong(), args[1], args[2], args[3], birthDate)
                                } catch (e: Exception) {
                                    System.err.println("Error: ${e.message}")
                                }
                            }
                        }
                        "purchase" -> {
                            println("> Purchasing Game...")
                            if (args.size < 2) {
                                System.err.println("Usage: purchase [playerId] [gameId]")
                            } else {
                                producer.sendPurchaseGame(args[0].toLong(), args[1].toLong())
                            }
                        }
                        "review" -> {
                            println("> Submitting Review...")
                            if (args.size < 3) {
                                System.err.println("Usage: review [playerId] [gameId] [rating(0-5)] [comment?]")
                            } else {
                                try {
                                    val rating = args[2].toInt()
                                    if (rating !in 0..5) {
                                        System.err.println("Error: Rating must be between 0 and 5. Got: $rating")
                                    } else {
                                        val comment = if (args.size > 3) args.drop(3).joinToString(" ") else null
                                        producer.sendReviewGame(args[0].toLong(), args[1].toLong(), rating, comment)
                                    }
                                } catch (e: NumberFormatException) {
                                    System.err.println("Error: Invalid rating number")
                                }
                            }
                        }
                        "install" -> {
                            println("> Installing Game...")
                            if (args.size < 3) {
                                System.err.println("Usage: install [playerId] [gameId] [platform]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                producer.sendInstallGame(args[0].toLong(), args[1].toLong(), args[2])
                            }
                        }
                        "update" -> {
                            println("> Updating Game...")
                            if (args.size < 3) {
                                System.err.println("Usage: update [playerId] [gameId] [platform]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                producer.sendUpdateGame(args[0].toLong(), args[1].toLong(), args[2], "")
                            }
                        }
                        "uninstall" -> {
                            println("> Uninstalling Game...")
                            if (args.size < 3) {
                                System.err.println("Usage: uninstall [playerId] [gameId] [platform] [comment?]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                val comment = if (args.size > 3) args.drop(3).joinToString(" ") else null
                                producer.sendUninstallGame(args[0].toLong(), args[1].toLong(), args[2], comment)
                            }
                        }
                        "playtime" -> {
                            if (args.isEmpty() || args[0] == "start") {
                                // Start timer
                                if (args.size < 3) {
                                    System.err.println("Usage: playtime start [playerId] [gameId]")
                                } else if (timerStartTime != null) {
                                    System.err.println("Error: Timer already running! Use 'playtime stop' first.")
                                } else {
                                    timerPlayerId = args[1].toLong()
                                    timerGameId = args[2].toLong()
                                    timerStartTime = System.currentTimeMillis()
                                    println("> Playtime timer STARTED for Player=${timerPlayerId}, Game=${timerGameId}")
                                    println("  Use 'playtime stop' to stop the timer and send the event")
                                }
                            } else if (args[0] == "stop") {
                                // Stop timer
                                if (timerStartTime == null) {
                                    System.err.println("Error: No timer running! Use 'playtime start' first.")
                                } else {
                                    val endTime = System.currentTimeMillis()
                                    val elapsedMillis = endTime - timerStartTime!!
                                    val elapsedMinutes = (elapsedMillis / 1000 / 60).toLong()
                                    val elapsedSeconds = (elapsedMillis / 1000 % 60)
                                    
                                    println("> Playtime timer STOPPED")
                                    println("  Elapsed time: ${elapsedMinutes}m ${elapsedSeconds}s (${elapsedMillis}ms)")
                                    println("  Sending AddPlayTime event with duration...")
                                    
                                    // Send elapsed time in milliseconds (timespan)
                                    producer.sendAddPlayTime(timerPlayerId!!, timerGameId!!, elapsedMillis)
                                    
                                    // Reset timer
                                    timerStartTime = null
                                    timerPlayerId = null
                                    timerGameId = null
                                }
                            } else {
                                System.err.println("Usage: playtime start [playerId] [gameId]  OR  playtime stop")
                            }
                        }
                        "crash" -> {
                            println("> Reporting Crash...")
                            if (args.size < 5) {
                                System.err.println("Usage: crash [playerId] [gameId] [platform] [version] [errorCode] [message...]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                val message = args.drop(5).joinToString(" ")
                                producer.sendReportCrash(
                                    args[0].toLong(),
                                    args[1].toLong(),
                                    args[2],
                                    args[3],
                                    args[4].toLong(),
                                    message
                                )
                            }
                        }
                        "wishlist-add" -> {
                            println("> Adding to Wishlist...")
                            if (args.size < 2) {
                                System.err.println("Usage: wishlist-add [playerId] [gameId]")
                            } else {
                                producer.sendAddWishedGame(args[0].toLong(), args[1].toLong())
                            }
                        }
                        "wishlist-remove" -> {
                            println("> Removing from Wishlist...")
                            if (args.size < 2) {
                                System.err.println("Usage: wishlist-remove [playerId] [gameId]")
                            } else {
                                producer.sendRemoveWishedGame(args[0].toLong(), args[1].toLong())
                            }
                        }
                        "react" -> {
                            println("> Reacting to Review...")
                            if (args.size < 3) {
                                System.err.println("Usage: react [playerId] [reviewId] [0=NOTHING|1=POSITIVE|2=NEGATIVE]")
                            } else {
                                try {
                                    val reactType = args[2].toInt()
                                    if (reactType !in 0..2) {
                                        System.err.println("Error: ReactType must be 0 (NOTHING), 1 (POSITIVE), or 2 (NEGATIVE). Got: $reactType")
                                    } else {
                                        producer.sendReactReview(args[0].toLong(), args[1].toLong(), reactType)
                                    }
                                } catch (e: NumberFormatException) {
                                    System.err.println("Error: Invalid reactType number")
                                }
                            }
                        }
                        
                        // === DATABASE COMMANDS ===
                        "db-install" -> {
                            println("> Database: Adding installed game...")
                            if (args.size < 4) {
                                System.err.println("Usage: db-install [playerId] [gameId] [platform] [version]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                try {
                                    val installedGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
                                        .setId(null)
                                        .setPlayerId(args[0].toLong())
                                        .setGameId(args[1].toLong())
                                        .setPlatform(org.pops.et4.jvm.project.schemas.models.player.Platform.valueOf(args[2]))
                                        .setInstalledVersion(args[3])
                                        .build()
                                    
                                    val saved = installedGameRepository.save(installedGame)
                                    println("[Database] Installed game added: ID=${saved.id}, Player=${args[0]}, Game=${args[1]}, Platform=${args[2]}, Version=${args[3]}")
                                } catch (e: Exception) {
                                    System.err.println("[Database] Error: ${e.message}")
                                }
                            }
                        }
                        "db-update" -> {
                            println("> Database: Updating installed game version...")
                            if (args.size < 4) {
                                System.err.println("Usage: db-update [playerId] [gameId] [platform] [newVersion]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                try {
                                    val installedGames = installedGameRepository.findAll()
                                        .filter { it.playerId == args[0].toLong() && it.gameId == args[1].toLong() && it.platform?.toString() == args[2] }
                                    
                                    if (installedGames.isNotEmpty()) {
                                        val installedGame = installedGames.first()
                                        val updatedGame = org.pops.et4.jvm.project.schemas.models.player.InstalledGame.newBuilder()
                                            .setId(installedGame.id)
                                            .setPlayerId(installedGame.playerId)
                                            .setGameId(installedGame.gameId)
                                            .setPlatform(installedGame.platform)
                                            .setInstalledVersion(args[3])
                                            .build()
                                        
                                        val saved = installedGameRepository.save(updatedGame)
                                        println("[Database] Game updated: ID=${saved.id}, Player=${args[0]}, Game=${args[1]}, Platform=${args[2]}, NewVersion=${args[3]}")
                                    } else {
                                        System.err.println("[Database] Game not found in installed games")
                                    }
                                } catch (e: Exception) {
                                    System.err.println("[Database] Error: ${e.message}")
                                }
                            }
                        }
                        "db-uninstall" -> {
                            println("> Database: Removing installed game...")
                            if (args.size < 3) {
                                System.err.println("Usage: db-uninstall [playerId] [gameId] [platform]")
                            } else if (args[2] !in VALID_PLATFORMS) {
                                System.err.println("Error: Invalid platform '${args[2]}'. Valid platforms: ${VALID_PLATFORMS.joinToString(", ")}")
                            } else {
                                try {
                                    val installedGames = installedGameRepository.findAll()
                                        .filter { it.playerId == args[0].toLong() && it.gameId == args[1].toLong() && it.platform?.toString() == args[2] }
                                    
                                    if (installedGames.isNotEmpty()) {
                                        installedGameRepository.deleteAll(installedGames)
                                        println("[Database] Uninstalled game: Player=${args[0]}, Game=${args[1]}, Platform=${args[2]}")
                                    } else {
                                        System.err.println("[Database] Game not found in installed games")
                                    }
                                } catch (e: Exception) {
                                    System.err.println("[Database] Error: ${e.message}")
                                }
                            }
                        }
                        "get-installed" -> {
                            println("> Get installed games...")
                            installedGameRepository.findAll().forEach { println(it) }
                        }
                        else -> println("Invalid option. Please try again.")
                    }

                    Thread.sleep(500)
                }
            }
        }
    }

    private fun printMenu(timerActive: Boolean) {
        println()
        println("=================================")
        println("          Player Service         ")
        println("=================================")
        if (timerActive) {
            println("⏱️  TIMER RUNNING - Use 'playtime stop' to stop")
            println("=================================")
        }
        println("SYSTEM COMMANDS:")
        println("* Exit                          exit/quit")
        println("* Start Listener                start [listenerId...]")
        println("* Stop Listener                 stop [listenerId...]")
        println()
        println("PRODUCER EVENTS (Player sends):")
        println("* Register Player               register [distId] [pseudo] [first] [last] [birthDate]")
        println("* Purchase Game                 purchase [playerId] [gameId]")
        println("* Review Game                   review [playerId] [gameId] [rating(0-5)] [comment?]")
        println("* Install Game                  install [playerId] [gameId] [platform]")
        println("* Update Game                   update [playerId] [gameId] [platform]")
        println("* Uninstall Game                uninstall [playerId] [gameId] [platform] [comment?]")
        println("* Start Play Time Timer         playtime start [playerId] [gameId]")
        println("* Stop Play Time Timer          playtime stop")
        println("* Report Crash                  crash [pId] [gId] [platform] [ver] [code] [msg...]")
        println("* Add to Wishlist               wishlist-add [playerId] [gameId]")
        println("* Remove from Wishlist          wishlist-remove [playerId] [gameId]")
        println("* React to Review               react [playerId] [reviewId] [0=NOTHING|1=POSITIVE|2=NEGATIVE]")
        println()
        println("DATABASE COMMANDS:")
        println("* Add Installed Game            db-install [playerId] [gameId] [platform] [version]")
        println("* Update Installed Game         db-update [playerId] [gameId] [platform] [newVersion]")
        println("* Remove Installed Game         db-uninstall [playerId] [gameId] [platform]")
        println("* Get Installed Games           get-installed")
        println()
        print("> ")
    }
}

fun main(args: Array<String>) {
    runApplication<App>(*args)
}