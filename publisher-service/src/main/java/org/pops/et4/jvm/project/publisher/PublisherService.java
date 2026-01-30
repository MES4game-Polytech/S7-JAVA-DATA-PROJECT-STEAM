package org.pops.et4.jvm.project.publisher;

import org.pops.et4.jvm.project.publisher.kafka.KafkaProducerService;
import org.pops.et4.jvm.project.schemas.events.CrashReported;
import org.pops.et4.jvm.project.schemas.events.GameReviewed;
import org.pops.et4.jvm.project.schemas.models.publisher.*;
import org.pops.et4.jvm.project.schemas.repositories.publisher.ReviewRepository;
import org.pops.et4.jvm.project.schemas.repositories.publisher.GameRepository;
import org.pops.et4.jvm.project.schemas.repositories.publisher.CrashReportRepository;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;


import java.util.ArrayList;
import java.util.List;

@Service(PublisherService.BEAN_NAME)
public class PublisherService {
    public static final String BEAN_NAME = "publisherService";

    private final GameRepository gameRepository;
    private final ReviewRepository reviewRepository;
    private final CrashReportRepository crashReportRepository;
    private final KafkaProducerService producerService;

    @Autowired
    public PublisherService(@Qualifier(GameRepository.BEAN_NAME) GameRepository gameRepository,
                            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository,
                            @Qualifier(CrashReportRepository.BEAN_NAME) CrashReportRepository crashReportRepository,
                            @Qualifier(KafkaProducerService.BEAN_NAME) KafkaProducerService producerService)
    {
        this.gameRepository = gameRepository;
        this.reviewRepository = reviewRepository;
        this.crashReportRepository = crashReportRepository;
        this.producerService = producerService;
    }

    /**
     * Enregistre une review reçue du Distributor
     */
    public void processGameReview(GameReviewed event) {
        Game game = gameRepository.findById(event.getGameId())
                .orElseThrow(() -> new RuntimeException("Game not found: " + event.getGameId()));

        Review review = Review.newBuilder()
                .setGame(game)
                .setId(event.getReviewId())
                .setRating(event.getRating())
                .setComment(event.getComment())
                .setPublicationDate(event.getPublicationDate())
                .build();

        long negativeReviewsCount = reviewRepository.findAll().stream()
                .filter(r -> r.getGame().getId().equals(game.getId()))
                .filter(r -> r.getRating() <= 2)
                .count();

        if (negativeReviewsCount > 0 && negativeReviewsCount % 15 == 0) {
            publishAutoPatch(game, "NEGATIVE_FEEDBACK");
        }

        reviewRepository.save(review);
    }

    /**
     * Enregistre un crash report reçu du Distributor
     */
    public void processCrashReport(CrashReported event) {
        Game game = gameRepository.findById(event.getGameId())
                .orElseThrow(() -> new RuntimeException("Game not found: " + event.getGameId()));

        // Conversion manuelle entre le type Platform de l'event et celui de la base
        org.pops.et4.jvm.project.schemas.models.publisher.Platform platformForDb =
                org.pops.et4.jvm.project.schemas.models.publisher.Platform.valueOf(event.getPlatform().name());

        CrashReport crash = CrashReport.newBuilder()
                .setGame(game)
                .setDistributorId(event.getDistributorId())
                .setPlatform(platformForDb)
                .setVersion(event.getInstalledVersion())
                .setErrorCode(event.getErrorCode())
                .setMessage(event.getMessage())
                .setReportDate(java.time.Instant.now())
                .build();

        // --- LOGIQUE DE RÉACTION ---
        // On récupère tous les crashs de la DB et on filtre ceux du jeu actuel
        long crashCount = crashReportRepository.findAll().stream()
                .filter(c -> c.getGame().getId().equals(game.getId()))
                .count();

        System.out.println("[Publisher] Total crashs for " + game.getName() + " : " + crashCount);

        // Réaction : seuil de 10 atteint, modulo 10 d'ailleurs pour chaque dizaine (10, 20, 30...)
        if (crashCount > 0 && crashCount % 10 == 0) {
            this.publishAutoPatch(game, "CRASH");
        }
        crashReportRepository.save(crash);
    }

    private void publishAutoPatch(Game game, String reasonType) {
        String currentVersion = game.getVersion();
        String nextVersion = incrementVersion(currentVersion);

        String description;
        List<LogTag> tags = new ArrayList<>();

        if ("CRASH".equals(reasonType)) {
            description = "Automatic stability fix after multiple crash reports.";
            tags.add(LogTag.BUG_FIX);
            tags.add(LogTag.SECURITY_FIX);
        } else {
            description = "Balance update based on community feedback.";
            tags.add(LogTag.ADD_FEATURE);
            tags.add(LogTag.BUG_FIX);
        }
        // On met à jour l'entité Game en base
        Patch newPatch = Patch.newBuilder()
                .setGame(game)
                .setVersion(nextVersion)
                .setTags(tags)
                .setDescription(description)
                .setPublicationDate(java.time.Instant.now())
                .build();

        // On met à jour l'entité Game en base
        Game updatedGame = Game.newBuilder(game)
                .setVersion(nextVersion)
                .build();
        gameRepository.save(updatedGame);

        producerService.sendPatchPublished(game.getId(), nextVersion);
    }

    private String incrementVersion(String currentVersion){
        try {
            String[] parts = currentVersion.split("\\.");

            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            int patch = Integer.parseInt(parts[2]);
            // Logique de la retenue
            if (patch >= 9) {
                minor++; // On incrémente le deuxième chiffre
                patch = 0; // On réinitialise le dernier
            } else {
                patch++; // Sinon, on incrémente juste le dernier
            }

            return major + "." + minor + "." + patch;
        } catch (Exception e) {
            // En cas d'erreur de format (ex: "1.0"), on retourne un format propre
            return "1.1.0";
        }
    }
}
