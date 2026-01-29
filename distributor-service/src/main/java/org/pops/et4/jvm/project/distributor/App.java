package org.pops.et4.jvm.project.distributor;

import org.pops.et4.jvm.project.distributor.kafka.KafkaLifecycleService;
import org.pops.et4.jvm.project.distributor.kafka.KafkaProducerService;
import org.pops.et4.jvm.project.schemas.models.distributor.Distributor;
import org.pops.et4.jvm.project.schemas.repositories.distributor.DistributorRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;

@SpringBootApplication
public class App {

    public static final String CLI_BEAN_NAME = "distributorServiceCLI";

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean(name = App.CLI_BEAN_NAME)
    public CommandLineRunner interactiveTestRunner(
            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producer,
            @Qualifier(KafkaLifecycleService.BEAN_NAME) KafkaLifecycleService lifecycle,
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository,
            @Qualifier(DistributorService.BEAN_NAME) DistributorService distributorService,
            @Qualifier("distributorDbDistributedGameRepository") org.pops.et4.jvm.project.schemas.repositories.distributor.DistributedGameRepository distributedGameRepository,
            @Qualifier("distributorDbPlayerRepository") org.pops.et4.jvm.project.schemas.repositories.distributor.PlayerRepository playerRepository,
            @Qualifier("distributorDbOwnedGameRepository") org.pops.et4.jvm.project.schemas.repositories.distributor.OwnedGameRepository ownedGameRepository,
            @Qualifier("distributorDbReviewRepository") org.pops.et4.jvm.project.schemas.repositories.distributor.ReviewRepository reviewRepository
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

                        case "send":
                            System.out.println("> Sending 'ExampleEvent'...");
                            producer.sendExampleEvent(String.join(" ", args));
                            break;

                        case "get-distributor":
                            System.out.println("> Get distributors...");
                            for (Distributor distributor: distributorRepository.findAll())
                                System.out.println(distributor);
                            break;

                        case "add-distributor":
                            System.out.println("> Adding new distributor...");
                            if (args.length < 1) {
                                System.err.println("Too few arguments, required: 1");
                                break;
                            }
                            Distributor entity = Distributor.newBuilder()
                                    .setId(null)
                                    .setName(args[0])
                                    .build();
                            Distributor distributor1 = distributorRepository.save(entity);
                            System.out.println("Added distributor: " + distributor1);
                            break;

                        case "remove-distributor":
                            System.out.println("> Removing distributor(s)...");
                            for (String distributorId: args) {
                                try {
                                    Long id = Long.parseLong(distributorId);
                                    Optional<Distributor> distributorOpt = distributorRepository.findById(id);
                                    if (distributorOpt.isEmpty()) throw new RuntimeException();
                                    Distributor distributor2 = distributorOpt.get();
                                    distributorRepository.delete(distributor2);
                                    System.out.println("Removed distributor: " + distributor2);
                                } catch (NumberFormatException e) {
                                    Optional<Distributor> distributorOpt = distributorRepository.findFirstByName(distributorId);
                                    if (distributorOpt.isEmpty()) {
                                        System.out.println("Error: '" + distributorId + "' is not a valid distributor ID.");
                                        break;
                                    }
                                    Distributor distributor2 = distributorOpt.get();
                                    distributorRepository.delete(distributor2);
                                    System.out.println("Removed distributor: " + distributor2);
                                }
                            }
                            break;

                        case "start-sale":
                            System.out.println("> Starting sale...");
                            if (args.length < 3) {
                                System.err.println("Too few arguments, required: 3 (distributorId gameId salePercentage)");
                                break;
                            }
                            try {
                                Long distId = Long.parseLong(args[0]);
                                Long gId = Long.parseLong(args[1]);
                                Float salePerc = Float.parseFloat(args[2]);
                                distributorService.startSale(distId, gId, salePerc);
                                producer.sendSaleStarted(distId, gId, salePerc);
                                System.out.println("Sale started: " + salePerc * 100 + "% off on game " + gId);
                            } catch (NumberFormatException e) {
                                System.err.println("Invalid number format");
                            } catch (Exception e) {
                                System.err.println("Error: " + e.getMessage());
                            }
                            break;

                        case "list-games":
                            System.out.println("> Listing distributed games...");
                            if (args.length < 1) {
                                System.out.println("All distributed games:");
                                for (var game: distributedGameRepository.findAll())
                                    System.out.println(game);
                            } else {
                                try {
                                    Long distId = Long.parseLong(args[0]);
                                    System.out.println("Games for distributor " + distId + ":");
                                    for (var game: distributedGameRepository.findAll())
                                        if (game.getDistributor() != null && Objects.equals(game.getDistributor().getId(), distId))
                                            System.out.println(game);
                                } catch (NumberFormatException e) {
                                    System.err.println("Invalid distributor ID");
                                }
                            }
                            break;

                        case "list-players":
                            System.out.println("> Listing players...");
                            for (var player: playerRepository.findAll())
                                System.out.println(player);
                            break;

                        case "list-owned-games":
                            System.out.println("> Listing owned games...");
                            if (args.length < 1) {
                                System.out.println("All owned games:");
                                for (var owned: ownedGameRepository.findAll())
                                    System.out.println(owned);
                            } else {
                                try {
                                    Long playerId = Long.parseLong(args[0]);
                                    System.out.println("Games owned by player " + playerId + ":");
                                    for (var owned: ownedGameRepository.findAll())
                                        if (owned.getPlayer() != null && Objects.equals(owned.getPlayer().getId(), playerId))
                                            System.out.println(owned);
                                } catch (NumberFormatException e) {
                                    System.err.println("Invalid player ID");
                                }
                            }
                            break;

                        case "list-reviews":
                            System.out.println("> Listing reviews...");
                            if (args.length < 1) {
                                System.out.println("All reviews:");
                                for (var review: reviewRepository.findAll())
                                    System.out.println(review);
                            } else {
                                try {
                                    Long gameId = Long.parseLong(args[0]);
                                    System.out.println("Reviews for game " + gameId + ":");
                                    for (var review: reviewRepository.findAll())
                                        if (review.getGameId() == gameId)
                                            System.out.println(review);
                                } catch (NumberFormatException e) {
                                    System.err.println("Invalid game ID");
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

    private void printMenu() {
        System.out.println();
        System.out.println("=================================");
        System.out.println("       Distributor Service       ");
        System.out.println("=================================");
        System.out.println("* Exit                  exit/quit");
        System.out.println("* Start Listener        start [listenerId...]");
        System.out.println("* Stop Listener         stop [listenerId...]");
        System.out.println("* Send Payload          send [payload]");
        System.out.println();
        System.out.println("--- Distributors ---");
        System.out.println("* Get Distributors      get-distributor");
        System.out.println("* Add Distributor       add-distributor [name]");
        System.out.println("* Remove Distributor    remove-distributor [id/name...]");
        System.out.println();
        System.out.println("--- Games & Sales ---");
        System.out.println("* List Games            list-games [distributorId?]");
        System.out.println("* Start Sale            start-sale [distributorId] [gameId] [salePercentage]");
        System.out.println();
        System.out.println("--- Players & Reviews ---");
        System.out.println("* List Players          list-players");
        System.out.println("* List Owned Games      list-owned-games [playerId?]");
        System.out.println("* List Reviews          list-reviews [gameId?]");
        System.out.println();
        System.out.print("> ");
    }

}