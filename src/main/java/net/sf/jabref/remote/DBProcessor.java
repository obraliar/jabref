package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jabref.model.entry.BibEntry;

// TODO Exceptions/LOGGER

public class DBProcessor {

    // private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;

    private DBType dbType;
    private final DBHelper dbHelper;

    public static final String REMOTE_ID = "REMOTE_ID";
    public static final String ENTRYTYPE = "ENTRYTYPE";


    public DBProcessor(Connection connection, DBType dbType) {
        this.connection = connection;
        this.dbType = dbType;
        this.dbHelper = new DBHelper(connection);
    }

    public boolean checkIntegrity() {
        Map<String, String> requiredColumns = dbType.getStructure();

        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            try (ResultSet databaseMetaDataResultSet = databaseMetaData.getTables(null, null, null, null)) {

                Set<String> requiredTables = new HashSet<>();
                requiredTables.add("ENTRY");

                while (databaseMetaDataResultSet.next()) {
                    String tableName = databaseMetaDataResultSet.getString("TABLE_NAME").toUpperCase();
                    requiredTables.remove(tableName);
                }
                databaseMetaDataResultSet.close();

                if (requiredTables.isEmpty()) {
                    try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape("ENTRY", dbType))) {
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                            requiredColumns.remove(resultSetMetaData.getColumnName(i + 1).toUpperCase(),
                                        resultSetMetaData.getColumnTypeName(i + 1).toUpperCase());
                        }

                        return requiredColumns.size() == 0;
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    if (this.dbType == DBType.MYSQL) {
                        /*..*/
                    }
                }
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }


        return false;
        /*... TODO ... also for other types*/
    }

    public void setUpRemoteDatabase() {
        try {
            if (dbType == DBType.MYSQL) {
                connection.createStatement().executeUpdate(
                      "CREATE TABLE IF NOT EXISTS ENTRY ("
                    + "REMOTE_ID int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                    + "ENTRYTYPE VARCHAR(255) DEFAULT NULL"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;");
            } else if (dbType == DBType.POSTGRESQL) {
                connection.createStatement().executeUpdate(
                        "CREATE TABLE IF NOT EXISTS ENTRY ("
                      + "REMOTE_ID SERIAL PRIMARY KEY,"
                      + "ENTRYTYPE VARCHAR);");
            } else if (dbType == DBType.ORACLE) {
                connection.createStatement().executeUpdate(
                        "CREATE TABLE \"ENTRY\" ("
                        + "\"REMOTE_ID\"  NUMBER NOT NULL,"
                        + "\"ENTRYTYPE\"  VARCHAR2(255) NULL,"
                        + "CONSTRAINT  \"ENTRY_PK\" PRIMARY KEY (\"REMOTE_ID\"))");
                connection.createStatement().executeUpdate("CREATE SEQUENCE \"ENTRY_SEQ\"");
                connection.createStatement().executeUpdate(
                        "CREATE TRIGGER \"BI_ENTRY\" BEFORE INSERT ON \"ENTRY\" "
                        + "FOR EACH ROW BEGIN "
                        + "SELECT \"ENTRY_SEQ\".NEXTVAL INTO :NEW.\"REMOTE_ID\" FROM DUAL; "
                        + "END;");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (!checkIntegrity()) {
            System.err.println("Corrupt tables. Please fix manully or use other.");
        }

    }


    public void insertEntry(BibEntry bibEntry) {
        prepareEntryTableStructure(bibEntry);

        // Check if exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = dbHelper.query("SELECT * FROM "+ escape("ENTRY", dbType) +" WHERE "+ escape("REMOTE_ID", dbType) +" = " + remote_id)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }


        String query = "INSERT INTO " + escape("ENTRY", dbType) + "(";
        ArrayList<String> keyList = new ArrayList<>(bibEntry.getFieldNames());

        for (int i = 0; i < keyList.size(); i++) {
            query = query + escape(keyList.get(i).toUpperCase(), dbType) + ", ";
        }
        query = query + escape("ENTRYTYPE", dbType) + ") VALUES(";
        for (int i = 0; i < keyList.size(); i++) {
            query = query + "'" + bibEntry.getField(keyList.get(i)) + "', ";
        }
        query = query + "'" + bibEntry.getType() + "')";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query, new String[] {"remote_id"})) { // lower case for postgresql // distinguish case
            preparedStatement.executeUpdate();
            try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setRemoteId(generatedKeys.getInt(1));
                    System.out.println("---->" + generatedKeys.getInt(1));
                }
                preparedStatement.close();
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
        System.out.println(">>> SQL UPDATE");
        try {
            connection.createStatement().executeUpdate("UPDATE "+ escape("ENTRY", dbType) +" SET " + escape(column.toUpperCase(), dbType) + " = " + "'" + newValue + "' WHERE "+ escape("REMOTE_ID", dbType) +" = " + bibEntry.getRemoteId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeEntry(BibEntry bibEntry) {
        System.out.println(">>> SQL DELETE");
        try {
            connection.createStatement().executeUpdate("DELETE FROM " + escape("ENTRY", dbType) + " WHERE "
                    + escape("REMOTE_ID", dbType) + " = " + bibEntry.getRemoteId());
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
        Set<String> fieldNames = allToUpperCase(bibEntry.getFieldNames());
        fieldNames.removeAll(allToUpperCase(dbHelper.getColumnNames(escape("ENTRY", dbType))));

        try {
            if (dbType == DBType.ORACLE) {
                for (String fieldName : fieldNames) {
                    connection.createStatement().executeUpdate("ALTER TABLE "+ escape("ENTRY", dbType) +" ADD ("+ escape(fieldName, dbType) +" CLOB NULL)");
                }
            } else {
                for (String fieldName : fieldNames) {
                    connection.createStatement().executeUpdate("ALTER TABLE "+ escape("ENTRY", dbType) +" ADD "+ escape(fieldName, dbType) +" TEXT NULL DEFAULT NULL");
                }
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



    public List<BibEntry> getRemoteEntries() {
        ArrayList<BibEntry> remoteEntries = new ArrayList<>();
        try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape("ENTRY", dbType))) {
            Set<String> columns = allToUpperCase(dbHelper.getColumnNames(escape("ENTRY", dbType)));

            while (resultSet.next()) {
                BibEntry bibEntry = new BibEntry();
                for (String column : columns) {
                    if (column.equals("REMOTE_ID")) {
                        bibEntry.setRemoteId(resultSet.getInt(column));
                    } else if (column.equals("ENTRYTYPE")) {
                        bibEntry.setType(resultSet.getString(column));
                    } else {
                        String value = resultSet.getString(column);
                        if (value != null) {
                            bibEntry.setField(column.toLowerCase(), value);
                        }
                    }
                }
                remoteEntries.add(bibEntry);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return remoteEntries;
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

    public Set<String> allToUpperCase(Set<String> stringSet) {
        Set<String> upperCaseStringSet = new HashSet<>();
        for (String string : stringSet) {
            upperCaseStringSet.add(string.toUpperCase());
        }
        return upperCaseStringSet;
    }

    public String escape(String expression, DBType type) {
        if (type == DBType.ORACLE) {
            return "\"" + expression + "\"";
        } else if (type == DBType.MYSQL) {
            return "`" + expression + "`";
        }
        return expression;
    }


}
