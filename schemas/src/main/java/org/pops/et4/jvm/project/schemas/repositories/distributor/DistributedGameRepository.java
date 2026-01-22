package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.DistributedGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(DistributedGameRepository.BEAN_NAME)
public interface DistributedGameRepository extends JpaRepository<DistributedGame, Long> {
    public static final String BEAN_NAME = "distributorDbDistributedGameRepository";
}
