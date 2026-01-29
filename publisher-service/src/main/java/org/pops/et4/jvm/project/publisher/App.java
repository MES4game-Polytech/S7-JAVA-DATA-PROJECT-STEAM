package org.pops.et4.jvm.project.publisher;

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
            @Qualifier(GameRepository.BEAN_NAME) GameRepository gameRepository // Ajoute bien le repo Game ici
    ) {
        return ignored -> {
            Thread.sleep(1000);

            // --- AUTOMATISATION DE L'IMPORT AU DÉMARRAGE ---
            System.out.println("> Vérification de la base de données...");
            if (publisherRepository.count() == 0) {
                System.out.println("> Base vide. Lancement de l'importation automatique du CSV...");
                handleCsvImport(publisherRepository, gameRepository);
            } else {
                System.out.println("> Données déjà présentes (" + publisherRepository.count() + " éditeurs).");
            }

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
                            handlePublishGame(producer, args);
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

    private void handleCsvImport(PublisherRepository pubRepo, GameRepository gameRepo) {
        String csvFile = "src/main/resources/vgsales.csv";
        int count = 0;
        // Ajout de la variable manquante pour la date de sortie
        java.time.Instant now = java.time.Instant.now();

        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(csvFile))) {
            br.readLine(); // Ignorer le header

            String line;
            while ((line = br.readLine()) != null && count < 500) { // On peut monter à 500 pour un bon jeu de test
                String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                if (data.length >= 6) {
                    String gameName = data[1].replace("\"", "");
                    String platformStr = data[2].toUpperCase();
                    String genreStr = data[4].toUpperCase();
                    String publisherName = data[5].replace("\"", "");

                    // Mapping de l'Editeur
                    Publisher publisher = pubRepo.findFirstByName(publisherName)
                            .orElseGet(() -> pubRepo.save(Publisher.newBuilder()
                                    .setName(publisherName)
                                    .setIsCompany(true)
                                    .build()));

                    // Mapping de la Plateforme (Sécurisé)
                    Platform platform = mapPlatform(platformStr);

                    // Mapping du Genre (Sécurisé)
                    Genre genre = mapGenre(genreStr);

                    // Création du Jeu (si ton schéma Game le permet)
                    Game game = Game.newBuilder()
                            .setName(gameName)
                    .setPublisher(publisher)
                    .setVersion("1.0.0")
                    .setReleaseDate(now)
                    .setPlatforms(List.of(platform))
                    .setGenres(List.of(genre))
                    .build();

                    gameRepo.save(game);
                    count++;
                }
            }
            System.out.println("> Importation terminée avec succès !");
        } catch (Exception e) {
            System.err.println("> Erreur lors de l'importation : " + e.getMessage());
        }
    }

    // --- MÉTHODES DE MAPPING (HELPER) ---
    private Platform mapPlatform(String csvPlatform) {
        try {
            // Gérer les cas particuliers du CSV
            if (csvPlatform.equals("PC")) return Platform.WINDOWS;
            if (csvPlatform.contains("PS4")) return Platform.PS4;
            if (csvPlatform.contains("XONE")) return Platform.XBOX_ONE;

            return Platform.valueOf(csvPlatform);
        } catch (IllegalArgumentException e) {
            return Platform.WINDOWS; // Valeur par défaut si inconnu
        }
    }

    private Genre mapGenre(String csvGenre) {
        try {
            return Genre.valueOf(csvGenre);
        } catch (IllegalArgumentException e) {
            return Genre.ACTION; // Valeur par défaut
        }
    }

    private void handlePublishGame(KafkaProducerService producer, String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: publish-game [id] [name] [version]");
            return;
        }
        try {
            long id = Long.parseLong(args[0]);
            String name = args[1];
            String v = args[2];
            producer.sendGamePublished(id, name, v);
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
        System.out.println("* Start Listener      start [listenerId...]");
        System.out.println("* Stop Listener       stop [listenerId...]");
        System.out.println("\n[KAFKA PRODUCER EVENTS]");
        System.out.println("* Publish Game        publish-game [gameId] [name] [version]");
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