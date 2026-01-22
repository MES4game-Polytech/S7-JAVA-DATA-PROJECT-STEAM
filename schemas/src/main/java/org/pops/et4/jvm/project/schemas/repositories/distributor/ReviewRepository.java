package org.pops.et4.jvm.project.schemas.repositories.distributor;

import org.pops.et4.jvm.project.schemas.models.distributor.Review;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(ReviewRepository.BEAN_NAME)
public interface ReviewRepository extends JpaRepository<Review, Long> {
    public static final String BEAN_NAME = "distributorDbReviewRepository";
}
