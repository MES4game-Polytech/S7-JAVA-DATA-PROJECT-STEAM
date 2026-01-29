package org.pops.et4.jvm.project.distributor;

import org.pops.et4.jvm.project.schemas.events.RegisterPlayer;
import org.pops.et4.jvm.project.schemas.events.ReviewGame;
import org.pops.et4.jvm.project.schemas.events.GamePublished;
import org.pops.et4.jvm.project.schemas.events.PatchPublished;
import org.pops.et4.jvm.project.schemas.models.distributor.Distributor;
import org.pops.et4.jvm.project.schemas.models.distributor.DistributedGame;
import org.pops.et4.jvm.project.schemas.models.distributor.OwnedGame;
import org.pops.et4.jvm.project.schemas.models.distributor.Player;
import org.pops.et4.jvm.project.schemas.models.distributor.Review;
import org.pops.et4.jvm.project.schemas.repositories.distributor.DistributedGameRepository;
import org.pops.et4.jvm.project.schemas.repositories.distributor.DistributorRepository;
import org.pops.et4.jvm.project.schemas.repositories.distributor.OwnedGameRepository;
import org.pops.et4.jvm.project.schemas.repositories.distributor.PlayerRepository;
import org.pops.et4.jvm.project.schemas.repositories.distributor.ReviewRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;

@Service(DistributorService.BEAN_NAME)
public class DistributorService {
    
    public static final String BEAN_NAME = "distributorService";

    private final PlayerRepository playerRepository;
    private final DistributorRepository distributorRepository;
    private final ReviewRepository reviewRepository;
    private final DistributedGameRepository distributedGameRepository;
    private final OwnedGameRepository ownedGameRepository;

    @Autowired
    public DistributorService(
            @Qualifier(PlayerRepository.BEAN_NAME) PlayerRepository playerRepository,
            @Qualifier(DistributorRepository.BEAN_NAME) DistributorRepository distributorRepository,
            @Qualifier(ReviewRepository.BEAN_NAME) ReviewRepository reviewRepository,
            @Qualifier(DistributedGameRepository.BEAN_NAME) DistributedGameRepository distributedGameRepository,
            @Qualifier(OwnedGameRepository.BEAN_NAME) OwnedGameRepository ownedGameRepository
    ) {
        this.playerRepository = playerRepository;
        this.distributorRepository = distributorRepository;
        this.reviewRepository = reviewRepository;
        this.distributedGameRepository = distributedGameRepository;
        this.ownedGameRepository = ownedGameRepository;
    }

    /**
     * Registers a new player in the distributor's database.
     * @param event The RegisterPlayer event containing player information
     * @return The saved Player entity
     */
    public Player registerPlayer(RegisterPlayer event) {
        // Fetch the distributor entity
        Distributor distributor = distributorRepository.findById(event.getDistributorId())
                .orElseThrow(() -> new RuntimeException("Distributor not found: " + event.getDistributorId()));

        Player player = Player.newBuilder()
                .setId(null)
                .setDistributor(distributor)
                .setPseudo(event.getPseudo())
                .setFirstName(event.getFirstName())
                .setLastName(event.getLastName())
                .setBirthDate(event.getBirthDate())
                .setRegistrationDate(Instant.now())
                .setWishedGames(Collections.emptyList())
                .build();

        return playerRepository.save(player);
    }

    /**
     * Processes a game review submitted by a player.
     * Validates that the player has at least 5 hours of playtime before accepting the review.
     * @param event The ReviewGame event containing review information
     * @return The saved Review entity with its generated ID, or null if review is refused
     */
    public Review reviewGame(ReviewGame event) {
        // Fetch the player entity
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Check if player owns the game and has sufficient playtime (minimum 5 hours = 300 minutes)
        OwnedGame ownedGame = ownedGameRepository.findAll().stream()
                .filter(og -> og.getPlayer() != null &&
                             og.getPlayer().getId() != null &&
                             og.getPlayer().getId().equals(event.getPlayerId()) &&
                             og.getGameId() == event.getGameId())
                .findFirst()
                .orElse(null);

        if (ownedGame == null || ownedGame.getPlayTime() < 300) {
            // Review refused - player doesn't own the game or has less than 5 hours playtime
            throw new IllegalStateException("Review refused: Player must have at least 5 hours of playtime on the game");
        }

        Review review = Review.newBuilder()
                .setId(null)
                .setPlayer(player)
                .setGameId(event.getGameId())
                .setRating(event.getRating())
                .setComment(event.getComment())
                .setPublicationDate(Instant.now())
                .setPositiveReactions(Collections.emptyList())
                .setNegativeReactions(Collections.emptyList())
                .build();

        return reviewRepository.save(review);
    }

    /**
     * Processes a game published by a publisher.
     * Creates a DistributedGame in the distributor's database.
     * @param event The GamePublished event containing game information
     * @return The saved DistributedGame entity with its generated ID
     */
    public DistributedGame gamePublished(GamePublished event) {
        // Fetch the distributor entity
        Distributor distributor = distributorRepository.findById(event.getDistributorId())
                .orElseThrow(() -> new RuntimeException("Distributor not found: " + event.getDistributorId()));

        DistributedGame distributedGame = DistributedGame.newBuilder()
                .setId(null)
                .setDistributor(distributor)
                .setGameId(event.getGameId())
                .setVersion(event.getVersion())
                .setPrice(event.getPrice())
                .setSale(null)
                .build();

        return distributedGameRepository.save(distributedGame);
    }

    /**
     * Processes a patch published by a publisher.
     * Updates the version of an existing DistributedGame.
     * @param event The PatchPublished event containing patch information
     * @return The updated DistributedGame entity
     */
    public DistributedGame patchPublished(PatchPublished event) {
        // Fetch the distributor entity
        Distributor distributor = distributorRepository.findById(event.getDistributorId())
                .orElseThrow(() -> new RuntimeException("Distributor not found: " + event.getDistributorId()));

        // Find the DistributedGame by distributor and gameId
        DistributedGame distributedGame = distributedGameRepository.findAll().stream()
                .filter(dg -> dg.getDistributor() != null && 
                             dg.getDistributor().getId() != null &&
                             dg.getDistributor().getId().equals(event.getDistributorId()) &&
                             dg.getGameId() == event.getGameId())
                .findFirst()
                .orElseThrow(() -> new RuntimeException("DistributedGame not found for distributor: " + 
                                                       event.getDistributorId() + " and game: " + event.getGameId()));

        // Update the version
        DistributedGame updatedGame = DistributedGame.newBuilder(distributedGame)
                .setVersion(event.getNewVersion())
                .build();

        return distributedGameRepository.save(updatedGame);
    }

    /**
     * Starts a sale on a distributed game.
     * Updates the sale field and returns the updated game.
     * @param distributorId ID of the distributor
     * @param gameId ID of the game
     * @param salePercentage Sale percentage (0-1, e.g., 0.2 for 20% off)
     * @return The updated DistributedGame entity
     */
    public DistributedGame startSale(Long distributorId, Long gameId, Float salePercentage) {
        // Find the DistributedGame by distributor and gameId
        DistributedGame distributedGame = distributedGameRepository.findAll().stream()
                .filter(dg -> dg.getDistributor() != null && 
                             dg.getDistributor().getId() != null &&
                             dg.getDistributor().getId().equals(distributorId) &&
                             dg.getGameId() == gameId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("DistributedGame not found for distributor: " + 
                                                       distributorId + " and game: " + gameId));

        // Update the sale
        DistributedGame updatedGame = DistributedGame.newBuilder(distributedGame)
                .setSale(salePercentage)
                .build();

        return distributedGameRepository.save(updatedGame);
    }

    /**
     * Processes an install game request.
     * Verifies the player owns the game before sending game files.
     * @param playerId ID of the player
     * @param gameId ID of the game to install
     * @return The DistributedGame with version information to send
     */
    public DistributedGame processInstallGame(Long playerId, Long gameId) {
        // Verify player owns the game
        OwnedGame ownedGame = ownedGameRepository.findAll().stream()
                .filter(og -> og.getPlayer() != null &&
                             og.getPlayer().getId() != null &&
                             og.getPlayer().getId().equals(playerId) &&
                             og.getGameId() == gameId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Player does not own this game"));

        // Get the distributed game to retrieve version and distributor info
        Player player = playerRepository.findById(playerId)
                .orElseThrow(() -> new RuntimeException("Player not found: " + playerId));
        
        DistributedGame distributedGame = distributedGameRepository.findAll().stream()
                .filter(dg -> dg.getDistributor() != null &&
                             dg.getDistributor().getId() != null &&
                             player.getDistributor() != null &&
                             player.getDistributor().getId() != null &&
                             dg.getDistributor().getId().equals(player.getDistributor().getId()) &&
                             dg.getGameId() == gameId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Game not found in distributor's catalog"));

        return distributedGame;
    }

    /**
     * Processes a game update request.
     * Verifies the player owns the game and checks if an update is needed before sending updated game files.
     * @param playerId ID of the player
     * @param gameId ID of the game to update
     * @param installedVersion The version currently installed by the player
     * @return The DistributedGame with latest version information to send
     */
    public DistributedGame processUpdateGame(Long playerId, Long gameId, String installedVersion) {
        // Verify player owns the game
        OwnedGame ownedGame = ownedGameRepository.findAll().stream()
                .filter(og -> og.getPlayer() != null &&
                             og.getPlayer().getId() != null &&
                             og.getPlayer().getId().equals(playerId) &&
                             og.getGameId() == gameId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Player does not own this game"));

        // Get the distributed game to retrieve latest version
        Player player = playerRepository.findById(playerId)
                .orElseThrow(() -> new RuntimeException("Player not found: " + playerId));
        
        DistributedGame distributedGame = distributedGameRepository.findAll().stream()
                .filter(dg -> dg.getDistributor() != null &&
                             dg.getDistributor().getId() != null &&
                             player.getDistributor() != null &&
                             player.getDistributor().getId() != null &&
                             dg.getDistributor().getId().equals(player.getDistributor().getId()) &&
                             dg.getGameId() == gameId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Game not found in distributor's catalog"));

        // Check if update is needed
        if (installedVersion.equals(distributedGame.getVersion())) {
            throw new IllegalStateException("Game is already up to date (version: " + installedVersion + ")");
        }

        return distributedGame;
    }
}
