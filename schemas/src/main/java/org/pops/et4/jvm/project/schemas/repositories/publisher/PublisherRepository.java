package org.pops.et4.jvm.project.schemas.repositories.publisher;

import org.pops.et4.jvm.project.schemas.models.publisher.Publisher;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository(PublisherRepository.BEAN_NAME)
public interface PublisherRepository extends JpaRepository<Publisher, Long> {
    public static final String BEAN_NAME = "publisherDbPublisherRepository";

    Optional<Publisher> findFirstByName(String name);
}
