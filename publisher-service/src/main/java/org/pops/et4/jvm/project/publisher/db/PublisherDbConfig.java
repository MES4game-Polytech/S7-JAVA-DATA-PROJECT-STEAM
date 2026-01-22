package org.pops.et4.jvm.project.publisher.db;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration(PublisherDbConfig.BEAN_NAME)
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = PublisherDbConfig.REPOSITORIES_PACKAGE,
        entityManagerFactoryRef = PublisherDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME,
        transactionManagerRef = PublisherDbConfig.TRANSACTION_MANAGER_BEAN_NAME
)
public class PublisherDbConfig {

    public static final String BEAN_NAME = "publisherServicePublisherDbConfig";

    public static final String DATA_SOURCE_BEAN_NAME = "publisherServicePublisherDbDataSource";
    public static final String DATA_SOURCE_PROPS_BEAN_NAME = "publisherServicePublisherDbDataSourceProperties";
    public static final String ENTITY_MANAGER_FACTORY_BEAN_NAME = "publisherServicePublisherDbEntityManagerFactory";
    public static final String TRANSACTION_MANAGER_BEAN_NAME = "publisherServicePublisherDbTransactionManager";

    public static final String REPOSITORIES_PACKAGE = "org.pops.et4.jvm.project.schemas.repositories.publisher";
    public static final String MODELS_PACKAGE = "org.pops.et4.jvm.project.schemas.models.publisher";
    public static final String DATA_SOURCE_CONFIG = "spring.datasource.publisher";
    public static final String ENTITY_MANAGER_PERSISTENCE = "publisher";
    public static final String DB_MODE = "update";

    @Bean(name = PublisherDbConfig.DATA_SOURCE_PROPS_BEAN_NAME)
    @ConfigurationProperties(prefix = PublisherDbConfig.DATA_SOURCE_CONFIG)
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = PublisherDbConfig.DATA_SOURCE_BEAN_NAME)
    public DataSource dataSource() {
        return this.dataSourceProperties()
                .initializeDataSourceBuilder()
                .build();
    }

    @Bean(name = PublisherDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME)
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier(PublisherDbConfig.DATA_SOURCE_BEAN_NAME) DataSource dataSource
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put("hibernate.hbm2ddl.auto", PublisherDbConfig.DB_MODE);

        return builder
                .dataSource(dataSource)
                .packages(PublisherDbConfig.MODELS_PACKAGE)
                .persistenceUnit(PublisherDbConfig.ENTITY_MANAGER_PERSISTENCE)
                .properties(props)
                .build();
    }

    @Bean(name = PublisherDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public PlatformTransactionManager transactionManager(
            @Qualifier(PublisherDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME) EntityManagerFactory emf
    ) {
        return new JpaTransactionManager(emf);
    }
}