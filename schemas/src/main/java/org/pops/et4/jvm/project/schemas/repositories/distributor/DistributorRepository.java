package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.Distributor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository(DistributorRepository.BEAN_NAME)
public interface DistributorRepository extends JpaRepository<Distributor, Long> {
    public static final String BEAN_NAME = "distributorDbDistributorRepository";

    Optional<Distributor> findFirstByName(String name);
}
