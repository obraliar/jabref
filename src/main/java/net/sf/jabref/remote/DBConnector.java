package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBConnector {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);


    public static Connection getNewConnection(DBType dbType, String host, String database, String user, String password)
            throws ClassNotFoundException, SQLException {
        return getNewConnection(dbType, host, getDefaultPort(dbType), database, user, password);
    }

    public static Connection getNewConnection(DBType dbType, String host, int port, String database, String user,
            String password) throws ClassNotFoundException, SQLException {

        String url = "jdbc:";

        try {
            if (dbType == DBType.MYSQL) {
                Class.forName("com.mysql.jdbc.Driver");
                url = url + "mysql://" + host + ":" + port + "/" + database;
            } else if (dbType == DBType.ORACLE) {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                url = url + "oracle:thin:@" + host + ":" + port + ":" + database;
            } else if (dbType == DBType.POSTGRESQL) {
                Class.forName("org.postgresql.Driver");
                url = url + "postgresql://" + host + ":" + port + "/" + database;
            }
            DriverManager.setLoginTimeout(3);
            return DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Could not load JDBC driver: " + e.getMessage());
            throw e;
        } catch (SQLException e) {
            LOGGER.error("Could not connect to database: " +
                    e.getMessage() + " - Error code: " + e.getErrorCode());
            throw e;
        }
    }

    public static int getDefaultPort(DBType dbType) {
        if (dbType == DBType.MYSQL) {
            return 3306;
        }
        if (dbType == DBType.POSTGRESQL) {
            return 5432;
        }
        if (dbType == DBType.ORACLE) {
            return 1521;
        }
        return -1;
    }
}
