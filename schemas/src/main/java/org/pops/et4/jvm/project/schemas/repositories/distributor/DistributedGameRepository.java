package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.DistributedGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository(DistributedGameRepository.BEAN_NAME)
public interface DistributedGameRepository extends JpaRepository<DistributedGame, Long> {
    public static final String BEAN_NAME = "distributorDbDistributedGameRepository";
    
    @Query("SELECT dg FROM DistributedGame dg WHERE dg.distributor.id = :distributorId AND dg.gameId = :gameId")
    Optional<DistributedGame> findByDistributorIdAndGameId(@Param("distributorId") Long distributorId, @Param("gameId") Long gameId);
}
