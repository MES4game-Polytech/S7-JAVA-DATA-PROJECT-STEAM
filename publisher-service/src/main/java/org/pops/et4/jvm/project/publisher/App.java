package org.pops.et4.jvm.project.publisher;

import jakarta.transaction.Transactional;
import org.pops.et4.jvm.project.publisher.kafka.KafkaConsumerService;
import org.pops.et4.jvm.project.publisher.kafka.KafkaLifecycleService;
import org.pops.et4.jvm.project.publisher.kafka.KafkaProducerService;
import org.pops.et4.jvm.project.schemas.models.publisher.Genre;
import org.pops.et4.jvm.project.schemas.models.publisher.Platform;
import org.pops.et4.jvm.project.schemas.models.publisher.Publisher;
import org.pops.et4.jvm.project.schemas.repositories.publisher.PublisherRepository;
import org.pops.et4.jvm.project.schemas.models.publisher.Game;
import org.pops.et4.jvm.project.schemas.repositories.publisher.GameRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@SpringBootApplication
public class App {

    public static final String CLI_BEAN_NAME = "publisherServiceCLI";

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean(name = App.CLI_BEAN_NAME)
    public CommandLineRunner interactiveTestRunner(
            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producer,
            @Qualifier(KafkaLifecycleService.BEAN_NAME) KafkaLifecycleService lifecycle,
            @Qualifier(PublisherRepository.BEAN_NAME) PublisherRepository publisherRepository,
            @Qualifier(GameRepository.BEAN_NAME) GameRepository gameRepository
    ) {
        return ignored -> {
            Thread.sleep(1000);

            // --- AUTOMATISATION DE L'IMPORT AU DÉMARRAGE ---
            System.out.println("> Checking database status...");
            long currentPubCount = publisherRepository.count();
            System.out.println("> Current database state: " + currentPubCount + " publishers found.");
            System.out.println("> Synchronizing with vgsales.csv...");
            handleCsvImport(publisherRepository, gameRepository);

            try (Scanner scanner = new Scanner(System.in)) {
                boolean running = true;

                while (running) {
                    printMenu();
                    String input = scanner.nextLine().trim();
                    if (input.isEmpty()) continue;
                    String[] cmd = input.split("\\s+");
                    String[] args = Arrays.copyOfRange(cmd, 1, cmd.length);

                    System.out.println("---------------------------------");

                    switch (cmd[0]) {
                        case "exit":
                        case "quit":
                            System.out.println("Exiting Manual Test Mode...");
                            running = false;
                            break;

                        case "start":
                            System.out.println("> Starting Listener(s)");
                            for (String listenerId: args)
                                lifecycle.startListener(listenerId);
                            break;

                        case "stop":
                            System.out.println("> Stopping Listener(s)");
                            for (String listenerId: args)
                                lifecycle.stopListener(listenerId);
                            break;

                        // --- COMMANDES KAFKA PRODUCER ---
                        case "publish-game":
                            System.out.println("> Publishing Game event...");
                            handlePublishGame(producer, gameRepository, args);
                            break;

                        case "publish-patch":
                            System.out.println("> Publishing Patch event...");
                            handlePublishPatch(producer, args);
                            break;

                        case "send":
                            System.out.println("> Sending 'ExampleEvent'...");
                            producer.sendExampleEvent(String.join(" ", args));
                            break;
                        // --- COMMANDES BDD ---
                        case "get-publisher":
                            System.out.println("> Get publishers...");
                            for (Publisher publisher: publisherRepository.findAll())
                                System.out.println(publisher);
                            break;

                        case "add-publisher":
                            System.out.println("> Adding new publisher...");
                            if (args.length < 2) {
                                System.err.println("Too few arguments, required: 2");
                                break;
                            }
                            Publisher entity = Publisher.newBuilder()
                                    .setId(null)
                                    .setName(args[0])
                                    .setIsCompany(Objects.equals(args[1], "1"))
                                    .build();
                            Publisher publisher1 = publisherRepository.save(entity);
                            System.out.println("Added publisher: " + publisher1);
                            break;

                        case "remove-publisher":
                            System.out.println("> Removing publisher(s)...");
                            for (String publisherId: args) {
                                try {
                                    Long id = Long.parseLong(publisherId);
                                    Optional<Publisher> publisherOpt = publisherRepository.findById(id);
                                    if (publisherOpt.isEmpty()) throw new RuntimeException();
                                    Publisher publisher2 = publisherOpt.get();
                                    publisherRepository.delete(publisher2);
                                    System.out.println("Removed publisher: " + publisher2);
                                } catch (NumberFormatException e) {
                                    Optional<Publisher> publisherOpt = publisherRepository.findFirstByName(publisherId);
                                    if (publisherOpt.isEmpty()) {
                                        System.out.println("Error: '" + publisherId + "' is not a valid publisher ID.");
                                        break;
                                    }
                                    Publisher publisher2 = publisherOpt.get();
                                    publisherRepository.delete(publisher2);
                                    System.out.println("Removed publisher: " + publisher2);
                                }
                            }
                            break;

                        default:
                            System.out.println("Invalid option. Please try again.");
                    }

                    Thread.sleep(500);
                }
            }
        };
    }

    private static final String CSV_FILENAME = "mon_fichier_donnees.csv";

    @Transactional
    public void handleCsvImport(PublisherRepository pubRepo, GameRepository gameRepo) {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(App.CSV_FILENAME);

        if (inputStream == null) {
            throw new IllegalArgumentException("Le fichier " + App.CSV_FILENAME + " est introuvable dans les ressources.");
        }

        try (var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            long inserted = 0;

            reader.lines()
                    .skip(1)
                    .filter(line -> line != null && !line.isBlank())
                    .forEach(line -> {
                        String[] data = line.split(",");

                        if (data.length >= 6) {
                            String gameName = data[0].replace("\"", "").trim();
                            String platformStr = data[1].toUpperCase().trim();
                            String genreStr = data[3].toUpperCase().trim();
                            String publisherName = data[4].replace("\"", "").trim();

                            try {
                                // Mapping de l'Editeur
                                Publisher publisher = pubRepo.findFirstByName(publisherName)
                                        .orElseGet(() -> pubRepo.save(Publisher.newBuilder()
                                                .setId(null)
                                                .setName(publisherName)
                                                .setIsCompany(true)
                                                .build()
                                        ));

                                // Mapping de la Plateforme
                                Platform platform = mapPlatform(platformStr);

                                // Mapping du Genre
                                Genre genre = mapGenre(genreStr);

                                // Création du Jeu
                                Game game = Game.newBuilder()
                                        .setId(null)
                                        .setName(gameName)
                                        .setPublisher(publisher)
                                        .setVersion("1.0.0")
                                        .setReleaseDate(Instant.now())
                                        .setPlatforms(List.of(platform))
                                        .setGenres(List.of(genre))
                                        .build();

                                // Sauvegarde du jeu
                                gameRepo.save(game);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });

            System.out.println("Import terminé depuis les ressources : " + inserted + " ajoutés.");
        } catch (IOException e) {
            throw new RuntimeException("Erreur de lecture du fichier ressource", e);
        } catch (NumberFormatException e) {
            System.err.println("Erreur de format dans le CSV : " + e.getMessage());
        }
    }

    // --- MÉTHODES DE MAPPING (HELPER) ---
    private Platform mapPlatform(String csvPlatform) {
        try {
            return switch (csvPlatform) {
                case "WS", "PC" -> Platform.WINDOWS;
                case "2600" -> Platform.ATARI2600;
                case "SAT" -> Platform.SATURN;
                case "3DS" -> Platform.NINTENDO_3DS;
                case "WIIU" -> Platform.WII_U;
                case "SNES" -> Platform.SUPER_NES;
                case "DC" -> Platform.DREAM_CAST;
                case "3DO" -> Platform.INTERACTIVE_3D0;
                case "XB" -> Platform.XBOX;
                case "GB" -> Platform.GAME_BOY;
                case "GBA" -> Platform.GAME_BOY_ADVANCED;
                case "GC" -> Platform.GAME_CUBE;
                case "GEN" -> Platform.GENESIS;
                case "GG" -> Platform.GAME_GEAR;
                case "NG" -> Platform.NEO_GEO;
                case "SCD" -> Platform.SEGA_CD;
                case "TG16" -> Platform.TURBO_GRAF;
                default -> Platform.valueOf(csvPlatform);
            };
        } catch (IllegalArgumentException e) {
            return Platform.UNKNOWN; // Valeur par défaut si inconnu
        }
    }

    private Genre mapGenre(String csvGenre) {
        try {
            return switch (csvGenre) {
                case "ROLE-PLAYING" -> Genre.RPG;
                default -> Genre.valueOf(csvGenre);
            };
        } catch (IllegalArgumentException e) {
            return Genre.UNKNOWN; // Valeur par défaut
        }
    }

    private void handlePublishGame(KafkaProducerService producer, GameRepository gameRepo, String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: publish-game [gameId]");
            return;
        }
        try {
            long id = Long.parseLong(args[0]);
            gameRepo.findById(id).ifPresentOrElse(
                    game -> {
                        producer.sendGamePublished(game); // On envoie l'objet complet
                        System.out.println("> Game '" + game.getName() + "' send to the Distributor.");
                    },
                    () -> System.out.println("> Error : No games found with the ID " + id)
            );
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private void handlePublishPatch(KafkaProducerService producer, String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: publish-patch [gameId] [version]");
            return;
        }
        try {
            long gId = Long.parseLong(args[0]);
            String v = args[1];
            producer.sendPatchPublished(gId, v);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("=================================");
        System.out.println("        Publisher Service        ");
        System.out.println("=================================");
        System.out.println("\n[SYSTEM]");
        System.out.println("* Exit                exit/quit");
        System.out.println("\n[KAFKA CONSUMER EVENTS]");
        System.out.println("* Start Listener      start [listenerId...]");
        System.out.println("* Stop Listener       stop [listenerId...]");
        System.out.println("  -> IDs: " + KafkaConsumerService.EXAMPLE_EVENT_CONSUMER_BEAN_NAME);
        System.out.println("  -> " + KafkaConsumerService.GAME_REVIEWED_CONSUMER_BEAN_NAME);
        System.out.println("  -> " + KafkaConsumerService.CRASH_REPORTED_CONSUMER_BEAN_NAME);
        System.out.println("\n[KAFKA PRODUCER EVENTS]");
        System.out.println("* Publish Game        publish-game [gameId]");
        System.out.println("* Publish Patch       publish-patch [gameId] [version]");
        System.out.println("* Send Payload        send [payload]");
        System.out.println("\n[DATABASE]");
        System.out.println("* Get Publishers      get-publisher");
        System.out.println("* Add Publisher       add-publisher [name] [isCompany]");
        System.out.println("* Remove Publisher    remove-publisher [id/name...]");
        System.out.println();
        System.out.print("> ");
    }
}