package com.weds.database.syncserver.config;

import com.google.common.base.Strings;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration
@EnableConfigurationProperties({RabbitmqProperties.class,SyncDBProperties.class,ScheduleProperties.class})
public class SyncServerConfig {

    @Value("${app.datasource.oracle.url:null}")
    private String oracleUrl;

    @Value("${app.datasource.sql-server.url:null}")
    private String sqlServerUrl;

    @Bean
    TaskScheduler threadPoolTaskScheduler() {
        return new ThreadPoolTaskScheduler();
    }

    @Bean(name = "cdcContainer")
    SimpleMessageListenerContainer cdcContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        // 设置不要自动启动
        container.setAutoStartup(false);
        container.setConnectionFactory(connectionFactory);
        return container;
    }


    /**
     * pg-database config
     * @return
     */
    @Bean
    @Primary
    @ConfigurationProperties("app.datasource.pg")
    public DataSourceProperties pgDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * pg-database config
     * @return
     */
    @Bean(name = "pgDb")
    @Primary
    @ConfigurationProperties("app.datasource.pg")
    public DataSource pgDataSource() {
        return pgDataSourceProperties().initializeDataSourceBuilder().build();
    }

    /**
     * pg-database config
     * @param pgDataSource
     * @return
     */
    @Primary
    @Bean(name = "pgJdbcTemplate")
    public JdbcTemplate pgJdbcTemplate(@Qualifier("pgDb") DataSource pgDataSource) throws SQLException {
        return new JdbcTemplate(pgDataSource);
    }


    /**
     * oracle-database config
     * @return
     */
    @Bean
    @ConfigurationProperties("app.datasource.oracle")
    public DataSourceProperties oracleDataSourceProperties() {
        if (Strings.isNullOrEmpty(oracleUrl) || "null".equals(oracleUrl)) {
            return pgDataSourceProperties();
        } else {
            return new DataSourceProperties();
        }
    }

    /**
     * oracle-database config
     * @return
     */
    @Bean(name = "oracleDb")
    @ConfigurationProperties("app.datasource.oracle")
    public DataSource oracleDataSource() {
        return oracleDataSourceProperties().initializeDataSourceBuilder().build();
    }

    /**
     * oracle-database config
     * @param oracleDataSource
     * @return
     */
    @Bean(name = "oracleJdbcTemplate")
    public JdbcTemplate oracleJdbcTemplate(@Qualifier("oracleDb") DataSource oracleDataSource) {
        return new JdbcTemplate(oracleDataSource);
    }

    @Bean
    @ConfigurationProperties("app.datasource.sqlServer")
    public DataSourceProperties sqlServerDataSourceProperties() {
        if (Strings.isNullOrEmpty(sqlServerUrl) || "null".equals(sqlServerUrl)) {
            return pgDataSourceProperties();
        } else {
            return new DataSourceProperties();
        }
    }

    @Bean(name = "sqlServerDb")
    @ConfigurationProperties("app.datasource.sqlServer")
    public DataSource sqlServerDataSource() {
        return sqlServerDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "sqlServerJdbcTemplate")
    public JdbcTemplate sqlServerJdbcTemplate(@Qualifier("sqlServerDb") DataSource sqlServerDataSource) {
        return new JdbcTemplate(sqlServerDataSource);
    }

    @Bean
    public ScheduledExecutorService scheduledExecutorService() {
        return new ScheduledThreadPoolExecutor(1);
    }
}
