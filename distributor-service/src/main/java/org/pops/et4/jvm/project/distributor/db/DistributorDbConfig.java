package org.pops.et4.jvm.project.distributor.db;

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

@Configuration(DistributorDbConfig.BEAN_NAME)
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = DistributorDbConfig.REPOSITORIES_PACKAGE,
        entityManagerFactoryRef = DistributorDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME,
        transactionManagerRef = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME
)
public class DistributorDbConfig {

    public static final String BEAN_NAME = "distributorServicePublisherDbConfig";

	public static final String DATA_SOURCE_BEAN_NAME = "distributorServiceDistributorDbDataSource";
    public static final String DATA_SOURCE_PROPS_BEAN_NAME = "distributorServiceDistributorDbDataSourceProperties";
    public static final String ENTITY_MANAGER_FACTORY_BEAN_NAME = "distributorServiceDistributorDbEntityManagerFactory";
    public static final String TRANSACTION_MANAGER_BEAN_NAME = "distributorServiceDistributorDbTransactionManager";

    public static final String REPOSITORIES_PACKAGE = "org.pops.et4.jvm.project.schemas.repositories.distributor";
    public static final String MODELS_PACKAGE = "org.pops.et4.jvm.project.schemas.models.distributor";
    public static final String DATA_SOURCE_CONFIG = "spring.datasource.distributor";
    public static final String ENTITY_MANAGER_PERSISTENCE = "distributor";
    public static final String DB_MODE = "update";

    @Bean(name = DistributorDbConfig.DATA_SOURCE_PROPS_BEAN_NAME)
    @ConfigurationProperties(prefix = DistributorDbConfig.DATA_SOURCE_CONFIG)
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = DistributorDbConfig.DATA_SOURCE_BEAN_NAME)
    public DataSource dataSource() {
        return this.dataSourceProperties()
                .initializeDataSourceBuilder()
                .build();
    }

    @Bean(name = DistributorDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME)
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            EntityManagerFactoryBuilder builder,
            @Qualifier(DistributorDbConfig.DATA_SOURCE_BEAN_NAME) DataSource dataSource
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put("hibernate.hbm2ddl.auto", DistributorDbConfig.DB_MODE);

        return builder
                .dataSource(dataSource)
                .packages(DistributorDbConfig.MODELS_PACKAGE)
                .persistenceUnit(DistributorDbConfig.ENTITY_MANAGER_PERSISTENCE)
                .properties(props)
                .build();
    }

    @Bean(name = DistributorDbConfig.TRANSACTION_MANAGER_BEAN_NAME)
    public PlatformTransactionManager transactionManager(
            @Qualifier(DistributorDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME) EntityManagerFactory emf
    ) {
        return new JpaTransactionManager(emf);
    }
}