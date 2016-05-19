package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.jabref.model.entry.BibEntry;

// TODO Locking
// TODO Exceptions

public class DBHelper {

    // private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;

    private DBType dbType;


    public boolean checkIntegrity() {

        Map<String, String> requiredColumns = new HashMap<>();
        if (this.dbType == DBType.MYSQL) {
            requiredColumns.put("remote_id", "INT");
            requiredColumns.put("entrytype", "VARCHAR");
        } else if (this.dbType == DBType.POSTGRESQL) {
            requiredColumns.put("remote_id", "serial");
            requiredColumns.put("entrytype", "varchar");
        } else {
            requiredColumns.put("unknown", "unknown");
        }

        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            try (ResultSet databaseMetaDataResultSet = databaseMetaData.getTables(null, null, null, null)) {

                Set<String> requiredTables = new HashSet<>();
                requiredTables.add("entry");

                while (databaseMetaDataResultSet.next()) {
                    requiredTables.remove(databaseMetaDataResultSet.getString("TABLE_NAME"));
                }
                databaseMetaDataResultSet.close();

                if (requiredTables.isEmpty()) {
                    try (ResultSet resultSet = query("SELECT * FROM entry")) {
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                            requiredColumns.remove(resultSetMetaData.getColumnName(i + 1),
                                        resultSetMetaData.getColumnTypeName(i + 1));
                        }

                        return requiredColumns.size() == 0;
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    if (this.dbType == DBType.MYSQL) {
                        /*..*/
                    }
                }

                return false;
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }


        return false;
        /*... TODO ... also for other types*/
    }

    public void setUpRemoteDatabase(DBType dbType) {
        try {
            if (dbType == DBType.MYSQL) {
                connection.createStatement().executeUpdate(
                      "CREATE TABLE IF NOT EXISTS entry ("
                    + "remote_id int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                    + "entrytype varchar(255) DEFAULT NULL"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;");
            } else if (dbType == DBType.POSTGRESQL) {
                connection.createStatement().executeUpdate(
                        "CREATE TABLE IF NOT EXISTS entry ("
                      + "remote_id SERIAL PRIMARY KEY,"
                      + "entrytype varchar);");
            } else if (dbType == DBType.ORACLE) {
                connection.createStatement().executeUpdate(
                        "CREATE table \"entry\" (    "
                        + "\"REMOTE_ID\"  NUMBER NOT NULL,    "
                        + "\"ENTRYTYPE\"  VARCHAR2(4000),    "
                        + "constraint  \"entry_pk\" primary key (\"REMOTE_ID\"))");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void insertEntry(BibEntry bibEntry) {
        prepareEntryTableStructure(bibEntry);

        // Check if exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = query("SELECT * FROM entry WHERE remote_id = " + remote_id)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }


        String query = "INSERT INTO entry(";
        ArrayList<String> keyList = new ArrayList<>();
        keyList.addAll(bibEntry.getFieldNames());

        for (int i = 0; i < keyList.size(); i++) {
            query = query + keyList.get(i) + ", ";
        }
        query = query + "entrytype) VALUES(";
        for (int i = 0; i < keyList.size(); i++) {
            query = query + "'" + bibEntry.getField(keyList.get(i)) + "', ";
        }
        query = query + "'" + bibEntry.getType() + "')";


        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setRemoteId(generatedKeys.getInt(1));
                }
                statement.close();
                generatedKeys.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        System.out.println(">>> SQL INSERT: " + query);

    }


    // TODO Case when bibEntry was not synchronized and remote_id is -1
    public void updateEntry(BibEntry bibEntry, String column, String newValue) {
        prepareEntryTableStructure(bibEntry);
        System.out.println(">>> SQL UPDATE: " + "UPDATE entry SET " + column + " = " + "'"+ newValue  +"' WHERE remote_id = " + bibEntry.getRemoteId());
        try {
            connection.createStatement().executeUpdate("UPDATE entry SET " + column + " = " + "'"+ newValue  +"' WHERE remote_id = " + bibEntry.getRemoteId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeEntry(BibEntry bibEntry) {
        System.out.println(">>> SQL DELETE: " + "DELETE FROM entry WHERE remote_id = " + bibEntry.getRemoteId());
        try {
            connection.createStatement().executeUpdate("DELETE FROM entry WHERE remote_id = " + bibEntry.getRemoteId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  Prepares the database table for a new bibEntry.
     *  Learning table structure: Columns which are not available are going to be created.
     *
     *  @param bibEntry Entry which pretends which missing columns should be created.
     *
     */
    public void prepareEntryTableStructure(BibEntry bibEntry) {
        Set<String> fieldNames = bibEntry.getFieldNames();
        fieldNames.removeAll(getColumnNames());

        try {
            for (String fieldName : fieldNames) {
                connection.createStatement().executeUpdate("ALTER TABLE entry ADD " + fieldName + " TEXT NULL DEFAULT NULL");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  Deletes all unused columns where every entry has a value NULL.
     */
    public void normalizeEntryTable() {
        return;
    }


    //TODO Parameterize
    public Set<String> getColumnNames() {
        try (ResultSet resultSet = query("SELECT * FROM entry")) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int count = resultSetMetaData.getColumnCount();
            Set<String> columnNames = new HashSet<>();

            for (int i = 0; i < count; i++) {
                columnNames.add(resultSetMetaData.getColumnName(i + 1));
            }

            return columnNames;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArrayList<ArrayList<String>> getQueryResultMatrix(String query) throws Exception {
        try (ResultSet resultSet = query(query)) {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int j = rsmd.getColumnCount();
            ArrayList<ArrayList<String>> rows = new ArrayList<>();
            ArrayList<String> cols;

            cols = new ArrayList<>();
            for (int i = 0; i < j; i++) {
                cols.add(rsmd.getColumnLabel(i + 1));
            }
            rows.add(cols);

            while (resultSet.next()) {
                cols = new ArrayList<>();
                for (int i = 0; i < j; i++) {
                    cols.add(resultSet.getString(i + 1));
                }
                rows.add(cols);
            }
            resultSet.close();
            return rows;

        } catch (Exception e) {
            throw e;
        }
    }

    public ResultSet query(String query) {
        try {
            return connection.createStatement().executeQuery(query);
        } catch (SQLException sqle) {
            System.err.println("SQLException: " + sqle.getMessage());
            return null;
        }
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void setDBType(DBType dbType) {
        this.dbType = dbType;
    }

    public DBType getDBType() {
        return this.dbType;
    }
}
