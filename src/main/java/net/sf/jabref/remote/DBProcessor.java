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

import net.sf.jabref.event.location.EntryEventLocation;
import net.sf.jabref.model.entry.BibEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBProcessor {

    //TODO Ausgaben entfernen
    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;

    private DBType dbType;
    private final DBHelper dbHelper;

    public static final String REMOTE_ID = "REMOTE_ID";
    public static final String ENTRYTYPE = "ENTRYTYPE";
    public static final String ENTRY = "ENTRY";



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
                requiredTables.add(ENTRY);

                while (databaseMetaDataResultSet.next()) {
                    String tableName = databaseMetaDataResultSet.getString("TABLE_NAME").toUpperCase();
                    requiredTables.remove(tableName);
                }
                databaseMetaDataResultSet.close();

                if (requiredTables.isEmpty()) {
                    try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape(ENTRY, dbType))) {
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                            requiredColumns.remove(resultSetMetaData.getColumnName(i + 1).toUpperCase(),
                                        resultSetMetaData.getColumnTypeName(i + 1).toUpperCase());
                        }

                        return requiredColumns.size() == 0;
                    } catch (SQLException e) {
                        LOGGER.error("SQL Error: " + e.getMessage());
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
                      "CREATE TABLE IF NOT EXISTS " + ENTRY +" ("
                                + REMOTE_ID + " INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,"
                                + ENTRYTYPE + " VARCHAR(255) DEFAULT NULL"
                    + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;");
            } else if (dbType == DBType.POSTGRESQL) {
                connection.createStatement().executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + ENTRY + " ("
                      + REMOTE_ID + " SERIAL PRIMARY KEY,"
                      + ENTRYTYPE + " VARCHAR);");
            } else if (dbType == DBType.ORACLE) {
                connection.createStatement().executeUpdate(
                        "CREATE TABLE \"" + ENTRY + "\" ("
                        + "\"" + REMOTE_ID + "\"  NUMBER NOT NULL,"
                        + "\"" + ENTRYTYPE + "\"  VARCHAR2(255) NULL,"
                        + "CONSTRAINT  \"ENTRY_PK\" PRIMARY KEY (\"" + REMOTE_ID + "\"))");
                connection.createStatement().executeUpdate("CREATE SEQUENCE \"" + ENTRY + "_SEQ\"");
                connection.createStatement().executeUpdate(
                        "CREATE TRIGGER \"BI_" + ENTRY + "\" BEFORE INSERT ON \"" + ENTRY + "\" "
                        + "FOR EACH ROW BEGIN "
                        + "SELECT \"" + ENTRY + "_SEQ\".NEXTVAL INTO :NEW." + REMOTE_ID.toLowerCase() + " FROM DUAL; "
                        + "END;");
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }

        if (!checkIntegrity()) {
            System.err.println("Corrupt tables. Please fix manully or use another.");
        }

    }


    public void insertEntry(BibEntry bibEntry) {
        prepareEntryTableStructure(bibEntry);

        // Check if exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = dbHelper.query("SELECT * FROM "+ escape(ENTRY, dbType) +" WHERE "+ escape(REMOTE_ID, dbType) +" = " + remote_id)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }


        String query = "INSERT INTO " + escape(ENTRY, dbType) + "(";
        ArrayList<String> keyList = new ArrayList<>(bibEntry.getFieldNames());

        for (int i = 0; i < keyList.size(); i++) {
            query = query + escape(keyList.get(i).toUpperCase(), dbType) + ", ";
        }
        query = query + escape(ENTRYTYPE, dbType) + ") VALUES(";
        for (int i = 0; i < keyList.size(); i++) {
            query = query + escapeValue(bibEntry.getField(keyList.get(i))) + ", ";
        }
        query = query + escapeValue(bibEntry.getType()) + ")";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query, new String[] {REMOTE_ID.toLowerCase()})) {
            preparedStatement.executeUpdate();
            try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setRemoteId(generatedKeys.getInt(1));
                }
                preparedStatement.close();
                generatedKeys.close();
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }

        System.out.println(">>> SQL INSERT: " + query);

    }


    public void updateEntry(BibEntry bibEntry, String column, String newValue) {
        prepareEntryTableStructure(bibEntry);

        try {
            connection.createStatement()
                    .executeUpdate("UPDATE " + escape(ENTRY, dbType) + " SET " + escape(column.toUpperCase(), dbType)
                            + " = " + escapeValue(newValue) + " WHERE " + escape(REMOTE_ID, dbType) + " = "
                            + bibEntry.getRemoteId());
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
    }

    public void removeEntry(BibEntry bibEntry) {
        System.out.println(">>> SQL DELETE");
        try {
            connection.createStatement().executeUpdate("DELETE FROM " + escape(ENTRY, dbType) + " WHERE "
                    + escape(REMOTE_ID, dbType) + " = " + bibEntry.getRemoteId());
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        normalizeEntryTable();
    }

    /**
     *  Prepares the database table for a new bibEntry.
     *  Learning table structure: Columns which are not available are going to be created.
     *
     *  @param bibEntry Entry which pretends which missing columns should be created.
     *
     */
    public void prepareEntryTableStructure(BibEntry bibEntry) {
        Set<String> fieldNames = dbHelper.allToUpperCase(bibEntry.getFieldNames());
        fieldNames.removeAll(dbHelper.allToUpperCase(dbHelper.getColumnNames(escape(ENTRY, dbType))));

        String columnType = dbType == DBType.ORACLE ? " CLOB NULL" : " TEXT NULL DEFAULT NULL";

        try {
            for (String fieldName : fieldNames) {
                connection.createStatement().executeUpdate(
                        "ALTER TABLE " + escape(ENTRY, dbType) + " ADD " + escape(fieldName, dbType) + columnType);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
    }

    /**
     *  Deletes all unused columns where every entry has a value NULL.
     */
    public void normalizeEntryTable() {
        ArrayList<String> columnsToRemove = new ArrayList<>();

        columnsToRemove.addAll(dbHelper.allToUpperCase(dbHelper.getColumnNames(escape(ENTRY, dbType))));
        columnsToRemove.remove(REMOTE_ID);
        columnsToRemove.remove(ENTRYTYPE);

        try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape(ENTRY, dbType))) {
            while (resultSet.next()) {
                for (int i = 0; i < columnsToRemove.size(); i++) {
                    String column = columnsToRemove.get(i);
                    if (resultSet.getObject(column) != null) {
                        columnsToRemove.remove(column);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }

        String columnExpression = "";
        String expressionPrefix = "";
        if ((dbType == dbType.MYSQL) || (dbType == dbType.POSTGRESQL)) {
            expressionPrefix = "DROP ";
        }

        for (int i = 0; i < columnsToRemove.size(); i++) {
            String column = columnsToRemove.get(i);
            columnExpression = columnExpression + expressionPrefix + escape(column, dbType);
            columnExpression = i < (columnsToRemove.size() - 1) ? columnExpression + ", " : columnExpression;
        }

        if (dbType == dbType.ORACLE) {
            columnExpression = "DROP (" + columnExpression + ")";
        }

        try {
            if (columnsToRemove.size() > 0) {
                connection.createStatement().executeUpdate("ALTER TABLE " + escape(ENTRY, dbType) + " " + columnExpression);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
    }



    public List<BibEntry> getRemoteEntries() {
        ArrayList<BibEntry> remoteEntries = new ArrayList<>();
        try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape(ENTRY, dbType))) {
            Set<String> columns = dbHelper.allToUpperCase(dbHelper.getColumnNames(escape(ENTRY, dbType)));

            while (resultSet.next()) {
                BibEntry bibEntry = new BibEntry();
                for (String column : columns) {
                    if (column.equals(REMOTE_ID)) {
                        bibEntry.setRemoteId(resultSet.getInt(column));
                    } else if (column.equals(ENTRYTYPE)) {
                        bibEntry.setType(resultSet.getString(column));
                    } else {
                        String value = resultSet.getString(column);
                        if (value != null) {
                            bibEntry.setField(column.toLowerCase(), value, EntryEventLocation.LOCAL);
                        }
                    }
                }
                remoteEntries.add(bibEntry);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        return remoteEntries;
    }

    public String escape(String expression, DBType type) {
        if (type == DBType.ORACLE) {
            return "\"" + expression + "\"";
        } else if (type == DBType.MYSQL) {
            return "`" + expression + "`";
        }
        return expression;
    }

    public String escapeValue(String value) {
        if (value == null) {
            value = "NULL";
        } else {
            value = "'" + value + "'";
        }
        return value;
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
