package com.cdc.student.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.springframework.stereotype.Component;
import io.debezium.config.Configuration;

@Component
public class ConnectionDB {

    public Connection connect(Configuration studentConnector) throws SQLException {
        Properties props = studentConnector.asProperties();
        String url = props.getProperty("datasource.database.url");
        String user = props.getProperty("datasource.database.user");
        String password = props.getProperty("datasource.database.password");

        return DriverManager.getConnection(url, user, password);       
    }
    
}
