package org.pops.et4.jvm.project.publisher;

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