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
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository
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
        System.out.println("* Get Distributors      get-distributor");
        System.out.println("* Add Distributor       add-distributor [name] [isCompany]");
        System.out.println("* Remove Distributor    remove-distributor [id/name...]");
        System.out.println();
        System.out.print("> ");
    }

}