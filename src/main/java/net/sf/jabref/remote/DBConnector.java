package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBConnector {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;

    public static Connection connect(DBType dbType, String host, String database, String user, String password) {

        String url = "jdbc:";

        try {
            if (dbType == DBType.MYSQL) {
                Class.forName("com.mysql.jdbc.Driver");
                url = url + "mysql://" + host + "/" + database;
            } else if (dbType == DBType.ORACLE) {
                Class.forName("org.postgresql.Driver");
                url = url + "oracle:thin:@" + host + ":" + database;
            } else if (dbType == DBType.POSTGRESQL) {
                Class.forName("org.postgresql.Driver");
                url = url + "postgresql://" + host + "/" + database;
            }
            return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Could not load JDBC driver: " + e.getMessage());
        } catch (SQLException e) {
            LOGGER.error("Could not connect to database: " +
                    e.getMessage() + "\nError code: " + e.getErrorCode());
        }
        return null;
    }

    public static boolean isNull(Connection connection) {
        return connection == null;
    }

    public Connection getConnection() {
        return this.connection;
    }
}