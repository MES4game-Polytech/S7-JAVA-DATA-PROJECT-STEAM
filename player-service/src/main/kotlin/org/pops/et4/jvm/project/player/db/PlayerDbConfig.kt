package org.pops.et4.jvm.project.player.db

import jakarta.persistence.EntityManagerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties
import org.springframework.boot.jpa.EntityManagerFactoryBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import javax.sql.DataSource

@Configuration(PlayerDbConfig.BEAN_NAME)
@EnableTransactionManagement
@EnableJpaRepositories(
    basePackages = [PlayerDbConfig.REPOSITORIES_PACKAGE],
    entityManagerFactoryRef = PlayerDbConfig.ENTITY_MANAGER_FACTORY_BEAN_NAME,
    transactionManagerRef = PlayerDbConfig.TRANSACTION_MANAGER_BEAN_NAME
)
class PlayerDbConfig {

    companion object {
        const val BEAN_NAME = "playerServicePlayerDbConfig"

        const val DATA_SOURCE_BEAN_NAME = "playerServicePlayerDbDataSource"
        const val DATA_SOURCE_PROPS_BEAN_NAME = "playerServicePlayerDbDataSourceProperties"
        const val ENTITY_MANAGER_FACTORY_BEAN_NAME = "playerServicePlayerDbEntityManagerFactory"
        const val TRANSACTION_MANAGER_BEAN_NAME = "playerServicePlayerDbTransactionManager"

        const val REPOSITORIES_PACKAGE = "org.pops.et4.jvm.project.schemas.repositories.player"
        const val MODELS_PACKAGE = "org.pops.et4.jvm.project.schemas.models.player"
        const val DATA_SOURCE_CONFIG = "spring.datasource.player"
        const val ENTITY_MANAGER_PERSISTENCE = "player"
        const val DB_MODE = "update"
    }

    @Bean(name = [DATA_SOURCE_PROPS_BEAN_NAME])
    @ConfigurationProperties(prefix = DATA_SOURCE_CONFIG)
    fun dataSourceProperties(): DataSourceProperties {
        return DataSourceProperties()
    }

    @Bean(name = [DATA_SOURCE_BEAN_NAME])
    fun dataSource(): DataSource {
        return dataSourceProperties()
            .initializeDataSourceBuilder()
            .build()
    }

    @Bean(name = [ENTITY_MANAGER_FACTORY_BEAN_NAME])
    fun entityManagerFactory(
        builder: EntityManagerFactoryBuilder,
        @Qualifier(DATA_SOURCE_BEAN_NAME) dataSource: DataSource
    ): LocalContainerEntityManagerFactoryBean {
        val props = mapOf("hibernate.hbm2ddl.auto" to DB_MODE)

        return builder
            .dataSource(dataSource)
            .packages(MODELS_PACKAGE)
            .persistenceUnit(ENTITY_MANAGER_PERSISTENCE)
            .properties(props)
            .build()
    }

    @Bean(name = [TRANSACTION_MANAGER_BEAN_NAME])
    fun transactionManager(
        @Qualifier(ENTITY_MANAGER_FACTORY_BEAN_NAME) emf: EntityManagerFactory
    ): PlatformTransactionManager {
        return JpaTransactionManager(emf)
    }
}