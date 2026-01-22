package org.pops.et4.jvm.project.schemas.repositories.publisher;

import org.pops.et4.jvm.project.schemas.models.publisher.Patch;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(PatchRepository.BEAN_NAME)
public interface PatchRepository extends JpaRepository<Patch, Long> {
    public static final String BEAN_NAME = "publisherDbPatchRepository";
}
