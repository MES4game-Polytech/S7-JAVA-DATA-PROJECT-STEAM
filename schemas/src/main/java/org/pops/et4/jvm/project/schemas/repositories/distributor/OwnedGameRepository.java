package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.OwnedGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(OwnedGameRepository.BEAN_NAME)
public interface OwnedGameRepository extends JpaRepository<OwnedGame, Long> {
    public static final String BEAN_NAME = "distributorDbOwnedGameRepository";
}
