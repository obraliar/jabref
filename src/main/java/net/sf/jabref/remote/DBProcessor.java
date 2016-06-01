/*  Copyright (C) 2003-2016 JabRef contributors.
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import net.sf.jabref.event.location.EntryEventLocation;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.entry.BibEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Processes all incoming or outgoing bib data to external SQL Database and manages its structure.
 */
public class DBProcessor {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;

    private DBType dbType;
    private final DBHelper dbHelper;

    public static final String ENTRY = "ENTRY";

    public static final List<String> ALL_TABLES = new ArrayList<>(Arrays.asList(ENTRY));

    // Elected column names of main the table
    // This entries are needed to ease the changeability, cause some database systems dependent on the context expect low or uppercase characters.
    public static final String REMOTE_ID = "REMOTE_ID";
    public static final String ENTRYTYPE = "ENTRYTYPE";



    /**
     * @param connection Working SQL connection
     * @param dbType Instance of {@link DBType}
     */
    public DBProcessor(Connection connection, DBType dbType) {
        this.connection = connection;
        this.dbType = dbType;
        this.dbHelper = new DBHelper(connection);
    }

    /**
     * Scans the database for required tables.
     * @return <code>true</code> if the structure matches the requirements, <code>false</code> if not.
     */
    public boolean checkBaseIntegrity() {
        List<String> requiredTables = new ArrayList<>(ALL_TABLES);
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            // ...getTables(null, ...): no restrictions
            try (ResultSet databaseMetaDataResultSet = databaseMetaData.getTables(null, null, null, null)) {

                while (databaseMetaDataResultSet.next()) {
                    String tableName = databaseMetaDataResultSet.getString("TABLE_NAME").toUpperCase();
                    requiredTables.remove(tableName); // Remove matching tables to check requiredTables for emptiness
                }

                databaseMetaDataResultSet.close();
                return requiredTables.size() == 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Creates and sets up the needed tables and columns according to the database type.
     */
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

        if (!checkBaseIntegrity()) {
            // can only happen with users direct intervention in remote database
            LOGGER.error(Localization.lang("Corrupt_remote_database_structure."));
        }

    }

    /**
     * Inserts the given bibEntry into remote database.
     * @param bibEntry {@link BibEntry} to be inserted
     */
    public void insertEntry(BibEntry bibEntry) {
        prepareEntryTableStructure(bibEntry);

        // Check if already exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = dbHelper.query("SELECT * FROM "+ escape(ENTRY, dbType) +" WHERE "+ escape(REMOTE_ID, dbType) +" = " + remote_id)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e) {
                LOGGER.error("SQL Error: " + e.getMessage());
            }
        }


        String query = "INSERT INTO " + escape(ENTRY, dbType) + "(";
        ArrayList<String> fieldNames = new ArrayList<>(bibEntry.getFieldNames());

        for (int i = 0; i < fieldNames.size(); i++) {
            query = query + escape(fieldNames.get(i).toUpperCase(), dbType) + ", ";
        }
        query = query + escape(ENTRYTYPE, dbType) + ") VALUES(";
        for (int i = 0; i < fieldNames.size(); i++) {
            query = query + escapeValue(bibEntry.getField(fieldNames.get(i))) + ", ";
        }
        query = query + escapeValue(bibEntry.getType()) + ")";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query,
                new String[] {REMOTE_ID.toLowerCase()})) { // This is the only method to get generated keys which is accepted by MySQL, PostgreSQL and Oracle.
            preparedStatement.executeUpdate();
            try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setRemoteId(generatedKeys.getInt(1)); // set generated ID locally
                }
                preparedStatement.close();
                generatedKeys.close();
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }

        LOGGER.info("SQL INSERT: " + query);
    }

    /**
     * Updates the remote existing bibEntry data.
     *
     * @param bibEntry {@link BibEntry} affected by changes
     * @param field Affected field name
     * @param newValue
     */
    public void updateEntry(BibEntry bibEntry, String field, String newValue) {
        prepareEntryTableStructure(bibEntry);
        String query = "UPDATE " + escape(ENTRY, dbType) + " SET " + escape(field.toUpperCase(), dbType) + " = "
                + escapeValue(newValue) + " WHERE " + escape(REMOTE_ID, dbType) + " = " + bibEntry.getRemoteId();
        try {
            connection.createStatement().executeUpdate(query);
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        LOGGER.info("SQL UPDATE: " + query);
    }

    /**
     * Removes the remote existing bibEntry
     * @param bibEntry {@link BibEntry} to be deleted
     */
    public void removeEntry(BibEntry bibEntry) {
        String query = "DELETE FROM " + escape(ENTRY, dbType) + " WHERE " + escape(REMOTE_ID, dbType) + " = "
                + bibEntry.getRemoteId();
        try {
            connection.createStatement().executeUpdate(query);
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
        LOGGER.info("SQL DELETE: " + query);
        normalizeEntryTable();
    }

    /**
     *  Prepares the database table for a new {@link BibEntry}.
     *  Learning table structure: Columns which are not available are going to be created.
     *
     *  @param bibEntry Entry which pretends missing columns which should be created.
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
        columnsToRemove.remove(REMOTE_ID); // essential column
        columnsToRemove.remove(ENTRYTYPE); // essential column

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
            columnExpression = "DROP (" + columnExpression + ")"; // DROP command in Oracle differs from the other systems.
        }

        try {
            if (columnsToRemove.size() > 0) {
                connection.createStatement().executeUpdate("ALTER TABLE " + escape(ENTRY, dbType) + " " + columnExpression);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: " + e.getMessage());
        }
    }


    /**
     * Converts all remotely present bib entries to the List of real {@link BibEntry} objects and retrieves them.
     */
    public List<BibEntry> getRemoteEntries() {
        ArrayList<BibEntry> remoteEntries = new ArrayList<>();
        try (ResultSet resultSet = dbHelper.query("SELECT * FROM " + escape(ENTRY, dbType))) {
            Set<String> columns = dbHelper.allToUpperCase(dbHelper.getColumnNames(escape(ENTRY, dbType)));

            while (resultSet.next()) {
                BibEntry bibEntry = new BibEntry();
                for (String column : columns) {
                    if (column.equals(REMOTE_ID)) { // distinguish, because special methods in BibEntry has to be used in this case
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

    /**
     * Escapes parts of SQL expressions like table or field name to match the conventions
     * of the database system.
     * @param expression Table or field name
     * @param type Type of database system
     * @return Correctly escape expression
     */
    public String escape(String expression, DBType type) {
        if (type == DBType.ORACLE) {
            return "\"" + expression + "\"";
        } else if (type == DBType.MYSQL) {
            return "`" + expression + "`";
        }
        return expression;
    }

    /**
     * Escapes the value indication of SQL expressions.
     *
     * @param Value to be escaped
     * @return Correctly escaped expression or "NULL" if <code>value</code> is real <code>null</code> object.
     */
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
