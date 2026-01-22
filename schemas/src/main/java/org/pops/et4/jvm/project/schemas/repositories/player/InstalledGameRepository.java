package org.pops.et4.jvm.project.schemas.repositories.player;

import org.pops.et4.jvm.project.schemas.models.player.InstalledGame;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(InstalledGameRepository.BEAN_NAME)
public interface InstalledGameRepository extends JpaRepository<InstalledGame, Long> {
    public static final String BEAN_NAME = "playerDbInstalledGameRepository";
}
