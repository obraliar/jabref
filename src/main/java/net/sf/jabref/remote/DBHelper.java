package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBHelper {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private final Connection connection;

    public DBHelper(Connection connection) {
        this.connection = connection;
    }

    public Set<String> getColumnNames(String table) {
        try (ResultSet resultSet = query("SELECT * FROM " + table)) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int count = resultSetMetaData.getColumnCount();
            Set<String> columnNames = new HashSet<>();

            for (int i = 0; i < count; i++) {
                columnNames.add(resultSetMetaData.getColumnName(i + 1));
            }

            return columnNames;

        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        return null;
    }

    public ResultSet query(String query) {
        try {
            return connection.createStatement().executeQuery(query);
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        return null;
    }

    public Set<String> allToUpperCase(Set<String> stringSet) {
        Set<String> upperCaseStringSet = new HashSet<>();
        for (String string : stringSet) {
            upperCaseStringSet.add(string.toUpperCase());
        }
        return upperCaseStringSet;
    }

}
