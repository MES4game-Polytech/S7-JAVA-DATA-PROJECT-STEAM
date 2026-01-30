package org.pops.et4.jvm.project.distributor;

import org.pops.et4.jvm.project.schemas.events.RegisterPlayer;
import org.pops.et4.jvm.project.schemas.events.ReviewGame;
import org.pops.et4.jvm.project.schemas.events.GamePublished;
import org.pops.et4.jvm.project.schemas.events.PatchPublished;
import org.pops.et4.jvm.project.schemas.events.*;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
     * Validates that the player has at least 15 seconds of playtime before accepting the review.
     * @param event The ReviewGame event containing review information
     * @return The saved Review entity with its generated ID, or null if review is refused
     */
    public Review reviewGame(ReviewGame event) {
        // Fetch the player entity
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Check if player owns the game and has sufficient playtime (minimum 15 seconds = 0.25 minutes)
        OwnedGame ownedGame = ownedGameRepository.findByPlayerIdAndGameId(event.getPlayerId(), event.getGameId())
                .orElse(null);

        if (ownedGame == null || ownedGame.getPlayTime() < 0.25) {
            // Review refused - player doesn't own the game or has less than 15 seconds playtime
            throw new IllegalStateException("Review refused: Player must have at least 15 seconds of playtime on the game");
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
     * Creates a DistributedGame for ALL distributors in the database.
     * @param event The GamePublished event containing game information
     * @return List of saved DistributedGame entities (one per distributor)
     */
    public List<DistributedGame> gamePublished(GamePublished event) {
        // Fetch ALL distributors and create a DistributedGame for each
        List<Distributor> distributors = distributorRepository.findAll();
        
        if (distributors.isEmpty()) {
            throw new RuntimeException("No distributors found in database");
        }

        List<DistributedGame> distributedGames = new ArrayList<>();
        
        for (Distributor distributor : distributors) {
            DistributedGame distributedGame = DistributedGame.newBuilder()
                    .setId(null)
                    .setDistributor(distributor)
                    .setGameId(event.getGameId())
                    .setGameName(event.getGameName())
                    .setVersion(event.getVersion())
                    .setPrice(59.99f) // Default price set by distributor
                    .setSale(null)
                    .build();

            distributedGames.add(distributedGameRepository.save(distributedGame));
        }

        return distributedGames;
    }

    /**
     * Processes a patch published by a publisher.
     * Updates the version of an existing DistributedGame for ALL distributors.
     * @param event The PatchPublished event containing patch information
     * @return List of updated DistributedGame entities (one per distributor)
     */
    public List<DistributedGame> patchPublished(PatchPublished event) {
        // Fetch ALL distributors
        List<Distributor> distributors = distributorRepository.findAll();
        
        if (distributors.isEmpty()) {
            throw new RuntimeException("No distributors found in database");
        }

        List<DistributedGame> updatedGames = new ArrayList<>();
        
        for (Distributor distributor : distributors) {
            // Find the DistributedGame by distributor and gameId
            distributedGameRepository.findByDistributorIdAndGameId(distributor.getId(), event.getGameId())
                    .ifPresent(distributedGame -> {
                        // Update the version
                        DistributedGame updatedGame = DistributedGame.newBuilder(distributedGame)
                                .setVersion(event.getVersion())
                                .build();

                        updatedGames.add(distributedGameRepository.save(updatedGame));
                    });
        }

        return updatedGames;
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
        DistributedGame distributedGame = distributedGameRepository.findByDistributorIdAndGameId(distributorId, gameId)
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
        OwnedGame ownedGame = ownedGameRepository.findByPlayerIdAndGameId(playerId, gameId)
                .orElseThrow(() -> new IllegalStateException("Player does not own this game"));

        // Get the distributed game to retrieve version and distributor info
        Player player = playerRepository.findById(playerId)
                .orElseThrow(() -> new RuntimeException("Player not found: " + playerId));
        
        DistributedGame distributedGame = distributedGameRepository.findByDistributorIdAndGameId(
                player.getDistributor().getId(), gameId)
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
        OwnedGame ownedGame = ownedGameRepository.findByPlayerIdAndGameId(playerId, gameId)
                .orElseThrow(() -> new IllegalStateException("Player does not own this game"));

        // Get the distributed game to retrieve latest version
        Player player = playerRepository.findById(playerId)
                .orElseThrow(() -> new RuntimeException("Player not found: " + playerId));
        
        DistributedGame distributedGame = distributedGameRepository.findByDistributorIdAndGameId(
                player.getDistributor().getId(), gameId)
                .orElseThrow(() -> new RuntimeException("Game not found in distributor's catalog"));

        // Check if update is needed
        if (installedVersion.equals(distributedGame.getVersion())) {
            throw new IllegalStateException("Game is already up to date (version: " + installedVersion + ")");
        }

        return distributedGame;
    }

    /**
     * Processes a crash report from a player.
     * Retrieves the distributor ID based on the player who reported the crash.
     * @param event The ReportCrash event containing crash information
     * @return The distributor ID to include in the CrashReported event
     */
    public Long processCrashReport(org.pops.et4.jvm.project.schemas.events.ReportCrash event) {
        // Fetch the player to get their distributor
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        if (player.getDistributor() == null || player.getDistributor().getId() == null) {
            throw new RuntimeException("Player has no associated distributor");
        }

        // Log the crash for tracking
        System.out.println("[Service] Crash reported for game " + event.getGameId() + 
                          " on platform " + event.getPlatform() + 
                          " with error code " + event.getErrorCode());

        return player.getDistributor().getId();
    }

    /**
     * Processes a game purchase.
     * Creates an OwnedGame entry in the database.
     * @param event The PurchaseGame event containing purchase information
     * @return The saved OwnedGame entity
     */
    public OwnedGame purchaseGame(org.pops.et4.jvm.project.schemas.events.PurchaseGame event) {
        // Fetch the player entity
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Create OwnedGame entry
        OwnedGame ownedGame = OwnedGame.newBuilder()
                .setId(null)
                .setPlayer(player)
                .setGameId(event.getGameId())
                .setPurchaseDate(Instant.now())
                .setPlayTime(0)
                .build();

        return ownedGameRepository.save(ownedGame);
    }

    /**
     * Adds playtime to a game that the player owns.
     * Updates the playTime field in OwnedGame.
     * @param event The AddPlayTime event containing playtime information
     * @return The updated OwnedGame entity
     */
    public OwnedGame addPlayTime(org.pops.et4.jvm.project.schemas.events.AddPlayTime event) {
        // Find the OwnedGame entry
        OwnedGame ownedGame = ownedGameRepository.findByPlayerIdAndGameId(event.getPlayerId(), event.getGameId())
                .orElseThrow(() -> new RuntimeException("OwnedGame not found for player: " + 
                                                       event.getPlayerId() + " and game: " + event.getGameId()));

        // Update playtime (event.getTime() is treated as duration in milliseconds, convert to minutes)
        int additionalPlayTimeMinutes = (int) (event.getTime() / 60000);
        OwnedGame updatedGame = OwnedGame.newBuilder(ownedGame)
                .setPlayTime(ownedGame.getPlayTime() + additionalPlayTimeMinutes)
                .build();

        return ownedGameRepository.save(updatedGame);
    }

    /**
     * Processes a reaction to a review.
     * Adds or removes player from positive/negative reactions based on reactType.
     * @param event The ReactReview event containing reaction information
     * @return The updated Review entity
     */
    public Review reactReview(org.pops.et4.jvm.project.schemas.events.ReactReview event) {
        // Fetch the review
        Review review = reviewRepository.findById(event.getReviewId())
                .orElseThrow(() -> new RuntimeException("Review not found: " + event.getReviewId()));

        // Fetch the player
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Get current reactions lists (create new mutable lists)
        java.util.List<Player> positiveReactions = new java.util.ArrayList<>(review.getPositiveReactions() != null ? review.getPositiveReactions() : Collections.emptyList());
        java.util.List<Player> negativeReactions = new java.util.ArrayList<>(review.getNegativeReactions() != null ? review.getNegativeReactions() : Collections.emptyList());

        // Remove player from both lists first
        positiveReactions.removeIf(p -> p.getId() != null && p.getId().equals(event.getPlayerId()));
        negativeReactions.removeIf(p -> p.getId() != null && p.getId().equals(event.getPlayerId()));

        // Add to appropriate list based on reactType: 0=NOTHING, 1=POSITIVE, 2=NEGATIVE
        if (event.getReactType() == 1) {
            positiveReactions.add(player);
        } else if (event.getReactType() == 2) {
            negativeReactions.add(player);
        }
        // If reactType == 0, player is removed from both (neutral reaction)

        // Update review
        Review updatedReview = Review.newBuilder(review)
                .setPositiveReactions(positiveReactions)
                .setNegativeReactions(negativeReactions)
                .build();

        return reviewRepository.save(updatedReview);
    }

    /**
     * Adds a game to a player's wishlist.
     * @param event The AddWishedGame event containing wishlist information
     * @return The updated Player entity
     */
    public Player addWishedGame(org.pops.et4.jvm.project.schemas.events.AddWishedGame event) {
        // Fetch the player
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Get current wishlist (create new mutable list)
        java.util.List<Long> wishedGames = new java.util.ArrayList<>(player.getWishedGames() != null ? player.getWishedGames() : Collections.emptyList());

        // Add game if not already in wishlist
        if (!wishedGames.contains(event.getGameId())) {
            wishedGames.add(event.getGameId());
        }

        // Update player
        Player updatedPlayer = Player.newBuilder(player)
                .setWishedGames(wishedGames)
                .build();

        return playerRepository.save(updatedPlayer);
    }

    /**
     * Removes a game from a player's wishlist.
     * @param event The RemoveWishedGame event containing wishlist information
     * @return The updated Player entity
     */
    public Player removeWishedGame(org.pops.et4.jvm.project.schemas.events.RemoveWishedGame event) {
        // Fetch the player
        Player player = playerRepository.findById(event.getPlayerId())
                .orElseThrow(() -> new RuntimeException("Player not found: " + event.getPlayerId()));

        // Get current wishlist (create new mutable list)
        java.util.List<Long> wishedGames = new java.util.ArrayList<>(player.getWishedGames() != null ? player.getWishedGames() : Collections.emptyList());

        // Remove game from wishlist
        wishedGames.remove(event.getGameId());

        // Update player
        Player updatedPlayer = Player.newBuilder(player)
                .setWishedGames(wishedGames)
                .build();

        return playerRepository.save(updatedPlayer);
    }

    /**
     * Retrieves the player name (pseudo) from the distributor database.
     * @param playerId ID of the player
     * @return The player pseudo
     */
    public String getPlayerName(Long playerId) {
        Player player = playerRepository.findById(playerId)
                .orElseThrow(() -> new RuntimeException("Player not found: " + playerId));
        return player.getPseudo();
    }

    /**
     * Retrieves the game name from the distributed games database.
     * @param gameId ID of the game
     * @return The game name
     */
    public String getGameName(Long gameId) {
        // Get the current distributor
        Distributor distributor = distributorRepository.findAll().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No distributor found in database"));
        
        DistributedGame game = distributedGameRepository.findByDistributorIdAndGameId(distributor.getId(), gameId)
                .orElseThrow(() -> new RuntimeException("Game not found in distributor catalog: " + gameId));
        return game.getGameName();
    }

    /**
     * Generates a formatted player page for the distributor.
     * @param distributorId ID of the distributor
     * @return Formatted player page as string
     */
    public String generatePlayerPage(Long distributorId) {
        // Find all players for this distributor
        java.util.List<Player> players = playerRepository.findAll().stream()
                .filter(p -> p.getDistributor() != null && p.getDistributor().getId().equals(distributorId))
                .toList();
        
        if (players.isEmpty()) {
            return "No players registered with this distributor.";
        }
        
        StringBuilder page = new StringBuilder();
        page.append("=================================\n");
        page.append("      PLAYERS DIRECTORY\n");
        page.append("=================================\n\n");
        
        for (Player player : players) {
            page.append("Player ID: ").append(player.getId()).append("\n");
            page.append("Name: ").append(player.getFirstName()).append(" ").append(player.getLastName()).append("\n");
            page.append("Pseudo: ").append(player.getPseudo()).append("\n");
            page.append("Registration Date: ").append(player.getRegistrationDate()).append("\n");
            
            // Get owned games count
            long ownedGamesCount = ownedGameRepository.findAll().stream()
                    .filter(og -> og.getPlayer().getId().equals(player.getId()))
                    .count();
            page.append("Owned Games: ").append(ownedGamesCount).append("\n");
            
            // Get total playtime
            int totalPlaytime = ownedGameRepository.findAll().stream()
                    .filter(og -> og.getPlayer().getId().equals(player.getId()))
                    .mapToInt(og -> og.getPlayTime())
                    .sum();
            page.append("Total Playtime: ").append(totalPlaytime).append(" minutes\n");
            
            // Get wishlist count
            int wishlistCount = player.getWishedGames() != null ? player.getWishedGames().size() : 0;
            page.append("Wishlist: ").append(wishlistCount).append(" games\n");
            
            page.append("---------------------------------\n");
        }
        
        return page.toString();
    }

    /**
     * Generates a JSON string containing all available games for a given platform from a distributor.
     * @param distributorId The distributor ID
     * @param platform The platform to filter games
     * @return JSON string with games information
     */
    public String buildGamesPage(Long distributorId, Platform platform) {
        // Verify distributor exists
        Distributor distributor = distributorRepository.findById(distributorId)
                .orElseThrow(() -> new RuntimeException("Distributor not found: " + distributorId));

        // Get all distributed games for this distributor and platform
        List<DistributedGame> games = distributedGameRepository.findAll().stream()
                .filter(game -> game.getDistributor().getId().equals(distributorId))
                .toList();

        StringBuilder page = new StringBuilder();
        page.append("=================================\n");
        page.append("   AVAILABLE GAMES - ").append(platform).append("\n");
        page.append("   Distributor: ").append(distributor.getName()).append("\n");
        page.append("=================================\n\n");

        if (games.isEmpty()) {
            page.append("No games available for this platform.\n");
        } else {
            for (DistributedGame game : games) {
                page.append("Game ID: ").append(game.getGameId()).append("\n");
                page.append("Name: ").append(game.getGameName()).append("\n");
                page.append("Version: ").append(game.getVersion()).append("\n");
                page.append("Price: $").append(game.getPrice()).append("\n");
                
                if (game.getSale() != null) {
                    float discountedPrice = game.getPrice() * (1 - game.getSale());
                    page.append("SALE! ").append((int)(game.getSale() * 100)).append("% OFF - ");
                    page.append("Now: $").append(String.format("%.2f", discountedPrice)).append("\n");
                }
                
                page.append("---------------------------------\n");
            }
        }

        return page.toString();
    }

    /**
     * Generates a JSON string containing all reviews for a specific game from a distributor.
     * @param distributorId The distributor ID
     * @param gameId The game ID
     * @return JSON string with reviews information
     */
    public String buildGameReviewsPage(Long distributorId, Long gameId) {
        // Verify distributor exists
        Distributor distributor = distributorRepository.findById(distributorId)
                .orElseThrow(() -> new RuntimeException("Distributor not found: " + distributorId));

        // Get the game
        DistributedGame game = distributedGameRepository.findByDistributorIdAndGameId(distributorId, gameId)
                .orElseThrow(() -> new RuntimeException("Game not found for distributor " + distributorId + " and game " + gameId));

        // Get all reviews for this game
        List<Review> reviews = reviewRepository.findAll().stream()
                .filter(review -> review.getGameId().equals(gameId))
                .toList();

        StringBuilder page = new StringBuilder();
        page.append("=================================\n");
        page.append("   GAME REVIEWS\n");
        page.append("   Game: ").append(game.getGameName()).append("\n");
        page.append("   Distributor: ").append(distributor.getName()).append("\n");
        page.append("=================================\n\n");

        if (reviews.isEmpty()) {
            page.append("No reviews yet for this game.\n");
        } else {
            page.append("Total Reviews: ").append(reviews.size()).append("\n");
            
            // Calculate average rating
            double avgRating = reviews.stream()
                    .mapToInt(Review::getRating)
                    .average()
                    .orElse(0.0);
            page.append("Average Rating: ").append(String.format("%.1f", avgRating)).append("/10\n\n");

            for (Review review : reviews) {
                page.append("Review ID: ").append(review.getId()).append("\n");
                page.append("Rating: ").append(review.getRating()).append("/10\n");
                page.append("Comment: ").append(review.getComment()).append("\n");
                page.append("Date: ").append(review.getPublicationDate()).append("\n");
                
                int positiveReactions = review.getPositiveReactions() != null ? review.getPositiveReactions().size() : 0;
                int negativeReactions = review.getNegativeReactions() != null ? review.getNegativeReactions().size() : 0;
                page.append("Reactions: 👍 ").append(positiveReactions)
                    .append(" | 👎 ").append(negativeReactions).append("\n");
                
                page.append("---------------------------------\n");
            }
        }

        return page.toString();
    }
}

