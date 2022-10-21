package com.cdc.student.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class provides the configurations required to setup a Debezium connector for the Student Table.
 *
 * @author Sohan
 */
@Configuration
public class DebeziumConnectorConfig {

    /**
     * Database details.
     */
    @Value("${student.datasource.host}")
    private String studentDBHost;

    @Value("${student.datasource.databasename}")
    private String studentDBName;

    @Value("${student.datasource.port}")
    private String studentDBPort;

    @Value("${student.datasource.username}")
    private String studentDBUserName;

    @Value("${student.datasource.password}")
    private String studentDBPassword;

    @Value("${project.id}")
    private String projectId;

    @Value("${topic.id}")
    private String topicId;
    
    @Value("${datasource.database.url}")
    private String dbUrl;

    @Value("${datasource.database.user}")
    private String dbUser;

    @Value("${datasource.database.password}")
    private String dbPassword;

    private String STUDENT_TABLE_NAME = "public.student";

    /**
     * database connector.
     *
     * @return Configuration.
     */
    @Bean
    public io.debezium.config.Configuration studentConnector() { 
        return io.debezium.config.Configuration.create()
                /* begin engine properties */
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage",  "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "/Users/User/Desktop/project_demo/engine-pubsub-seq-test/student-cdc-relay/student-offset.dat")
                .with("offset.flush.interval.ms", 60000)
                .with("name", "student-postgres-connector")
                .with("database.server.name", studentDBHost+"-"+studentDBName)
                /* begin connector properties */
                .with("database.hostname", studentDBHost)
                .with("database.port", studentDBPort)
                .with("database.user", studentDBUserName)
                .with("database.password", studentDBPassword)
                .with("database.dbname", studentDBName)
                .with("table.whitelist", STUDENT_TABLE_NAME)
                /* begin cloud pubsub properties */
                .with("project.id", projectId)
                .with("topic.id", topicId)
                /* begin db jdbc properties */
                .with("datasource.database.url",dbUrl)
                .with("datasource.database.user", dbUser)
                .with("datasource.database.password", dbPassword)
                .with("slot.drop.on.stop",false).build();
    }
}
