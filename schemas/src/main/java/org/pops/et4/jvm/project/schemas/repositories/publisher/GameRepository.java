package org.pops.et4.jvm.project.schemas.repositories.publisher;

import org.pops.et4.jvm.project.schemas.models.publisher.Game;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(GameRepository.BEAN_NAME)
public interface GameRepository extends JpaRepository<Game, Long> {
    public static final String BEAN_NAME = "publisherDbGameRepository";
}
