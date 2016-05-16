package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBHelper {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private final Connection connection;

    public DBHelper(Connection connection) {
        this.connection = connection;
    }

    public void checkIntegrity(DBType dbType) {
        if (dbType == DBType.MYSQL) {
            return;
        } /*... TODO ... also for other types*/
    }

    public void integrateDatabase() {

    }

    public void initializeLocalDatabase() {

    }

    public ResultSet query(String query) {
        try {
            return connection.createStatement().executeQuery(query);
        } catch (SQLException e) {
            LOGGER.error("SQLException: " + e.getMessage() + "\nError code: " + e.getErrorCode());
            return null;
        }
    }
}
