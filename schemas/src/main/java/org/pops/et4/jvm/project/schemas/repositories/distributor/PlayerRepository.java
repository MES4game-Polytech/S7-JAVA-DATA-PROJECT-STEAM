package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.Player;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(PlayerRepository.BEAN_NAME)
public interface PlayerRepository extends JpaRepository<Player, Long> {
    public static final String BEAN_NAME = "distributorDbPlayerRepository";
}
