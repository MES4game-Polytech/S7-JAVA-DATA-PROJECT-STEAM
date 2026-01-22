package org.pops.et4.jvm.project.schemas.repositories.publisher;

import org.pops.et4.jvm.project.schemas.models.publisher.CrashReport;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository(CrashReportRepository.BEAN_NAME)
public interface CrashReportRepository extends JpaRepository<CrashReport, Long> {
    public static final String BEAN_NAME = "publisherDbCrashReportRepository";
}
