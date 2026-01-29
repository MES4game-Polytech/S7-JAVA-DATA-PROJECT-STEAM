package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.OwnedGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository(OwnedGameRepository.BEAN_NAME)
public interface OwnedGameRepository extends JpaRepository<OwnedGame, Long> {
    public static final String BEAN_NAME = "distributorDbOwnedGameRepository";
    
    @Query("SELECT og FROM OwnedGame og WHERE og.player.id = :playerId AND og.gameId = :gameId")
    Optional<OwnedGame> findByPlayerIdAndGameId(@Param("playerId") Long playerId, @Param("gameId") Long gameId);
}
