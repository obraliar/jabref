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
package net.sf.jabref.shared;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import net.sf.jabref.event.source.EntryEventSource;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.entry.BibEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Processes all incoming or outgoing bib data to external SQL Database and manages its structure.
 */
public class DBMSProcessor {

    private static final Log LOGGER = LogFactory.getLog(DBMSConnector.class);

    private DBMSType dbmsType;
    private final DBMSHelper dbmsHelper;

    /**
     * @param connection Working SQL connection
     * @param dbmsType Instance of {@link DBMSType}
     */
    public DBMSProcessor(DBMSHelper dbmsHelper, DBMSType dbmsType) {
        this.dbmsType = dbmsType;
        this.dbmsHelper = dbmsHelper;
    }

    /**
     * Scans the database for required tables.
     * @return <code>true</code> if the structure matches the requirements, <code>false</code> if not.
     */
    public boolean checkBaseIntegrity() {
        List<String> requiredTables = new ArrayList<>(Arrays.asList("ENTRY", "FIELD", "METADATA")); // the list should be dynamic
        try {
            DatabaseMetaData databaseMetaData = dbmsHelper.getMetaData();

            // ...getTables(null, ...): no restrictions
            try (ResultSet databaseMetaDataResultSet = databaseMetaData.getTables(null, null, null, null)) {

                while (databaseMetaDataResultSet.next()) {
                    String tableName = databaseMetaDataResultSet.getString("TABLE_NAME").toUpperCase();
                    requiredTables.remove(tableName); // Remove matching tables to check requiredTables for emptiness
                }

                return requiredTables.isEmpty();
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
        return false;
    }


    /**
     * Creates and sets up the needed tables and columns according to the database type.
     */
    public void setUpRemoteDatabase() {
        if (dbmsType == DBMSType.MYSQL) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS `ENTRY` (" +
                "`REMOTE_ID` INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
                "`TYPE` VARCHAR(255) NOT NULL, " +
                "`VERSION` INT(11) DEFAULT 1)");

            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS `FIELD` (" +
                "`ENTRY_REMOTE_ID` INT(11) NOT NULL, " +
                "`NAME` VARCHAR(255) NOT NULL, " +
                "`VALUE` TEXT DEFAULT NULL, " +
                "FOREIGN KEY (`ENTRY_REMOTE_ID`) REFERENCES `ENTRY`(`REMOTE_ID`) ON DELETE CASCADE)");

            dbmsHelper.executeUpdate("CREATE TABLE IF NOT EXISTS `METADATA` (" +
                    "`KEY` varchar(255) NOT NULL," +
                    "`VALUE` text NOT NULL)");

        } else if (dbmsType == DBMSType.POSTGRESQL) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"ENTRY\" (" +
                "\"REMOTE_ID\" SERIAL PRIMARY KEY, " +
                "\"TYPE\" VARCHAR, " +
                "\"VERSION\" INTEGER DEFAULT 1)");

            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"FIELD\" (" +
                "\"ENTRY_REMOTE_ID\" INTEGER REFERENCES \"ENTRY\"(\"REMOTE_ID\") ON DELETE CASCADE, " +
                "\"NAME\" VARCHAR, " +
                "\"VALUE\" TEXT)");

            dbmsHelper.executeUpdate("CREATE TABLE IF NOT EXISTS \"METADATA\" ("
                    + "\"KEY\" VARCHAR,"
                    + "\"VALUE\" TEXT)");

        } else if (dbmsType == DBMSType.ORACLE) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE \"ENTRY\" (" +
                "\"REMOTE_ID\" NUMBER NOT NULL, " +
                "\"TYPE\" VARCHAR2(255) NULL, " +
                "\"VERSION\" NUMBER DEFAULT 1, " +
                "CONSTRAINT \"ENTRY_PK\" PRIMARY KEY (\"REMOTE_ID\"))");

            dbmsHelper.executeUpdate("CREATE SEQUENCE \"ENTRY_SEQ\"");

            dbmsHelper.executeUpdate("CREATE TRIGGER \"ENTRY_T\" BEFORE INSERT ON \"ENTRY\" " +
                "FOR EACH ROW BEGIN SELECT \"ENTRY_SEQ\".NEXTVAL INTO :NEW.remote_id FROM DUAL; END;");

            dbmsHelper.executeUpdate(
                "CREATE TABLE \"FIELD\" (" +
                "\"ENTRY_REMOTE_ID\" NUMBER NOT NULL, " +
                "\"NAME\" VARCHAR2(255) NOT NULL, " +
                "\"VALUE\" CLOB NULL, " +
                "CONSTRAINT \"ENTRY_REMOTE_ID_FK\" FOREIGN KEY (\"ENTRY_REMOTE_ID\") " +
                "REFERENCES \"ENTRY\"(\"REMOTE_ID\") ON DELETE CASCADE)");

            dbmsHelper.executeUpdate("CREATE TABLE \"METADATA\" (" +
                    "\"KEY\"  VARCHAR2(255) NULL," +
                    "\"VALUE\"  CLOB NOT NULL)");

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

        // Check if already exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = selectFromEntryTable(remote_id)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e) {
                LOGGER.error("SQL Error: ", e);
            }
        }

        // Inserting into ENTRY table
        StringBuilder insertIntoEntryQuery = new StringBuilder();
        insertIntoEntryQuery.append("INSERT INTO ");
        insertIntoEntryQuery.append(escape("ENTRY"));
        insertIntoEntryQuery.append("(");
        insertIntoEntryQuery.append(escape("TYPE"));
        insertIntoEntryQuery.append(") VALUES(?)");

        // This is the only method to get generated keys which is accepted by MySQL, PostgreSQL and Oracle.
        try (PreparedStatement preparedEntryStatement = dbmsHelper.prepareStatement(insertIntoEntryQuery.toString(),
                "REMOTE_ID")) {

            preparedEntryStatement.setString(1, bibEntry.getType());
            preparedEntryStatement.executeUpdate();

            try (ResultSet generatedKeys = preparedEntryStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setRemoteId(generatedKeys.getInt(1)); // set generated ID locally
                }
            }

            // Inserting into FIELD table
            for (String fieldName : bibEntry.getFieldNames()) {
                StringBuilder insertFieldQuery = new StringBuilder();
                insertFieldQuery.append("INSERT INTO ");
                insertFieldQuery.append(escape("FIELD"));
                insertFieldQuery.append("(");
                insertFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                insertFieldQuery.append(", ");
                insertFieldQuery.append(escape("NAME"));
                insertFieldQuery.append(", ");
                insertFieldQuery.append(escape("VALUE"));
                insertFieldQuery.append(") VALUES(?, ?, ?)");

                try (PreparedStatement preparedFieldStatement = dbmsHelper.prepareStatement(insertFieldQuery.toString())) {
                    // columnIndex starts with 1
                    preparedFieldStatement.setInt(1, bibEntry.getRemoteId());
                    preparedFieldStatement.setString(2, fieldName);
                    preparedFieldStatement.setString(3, bibEntry.getFieldOptional(fieldName).get());
                    preparedFieldStatement.executeUpdate();
                }
            }

        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    /**
     * Updates the whole {@link BibEntry} remotely.
     *
     * @param localBibEntry {@link BibEntry} affected by changes
     */
    public void updateEntry(BibEntry localBibEntry) throws OfflineLockException, RemoteEntryNotPresentException {
        dbmsHelper.setAutoCommit(false); // disable auto commit due to transaction

        try {
            Optional<BibEntry> remoteEntryOptional = getRemoteEntry(localBibEntry.getRemoteId());

            if (!remoteEntryOptional.isPresent()) {
                throw new RemoteEntryNotPresentException(localBibEntry);
            }

            BibEntry remoteBibEntry = remoteEntryOptional.get();

            // remove remote fields which does not exist locally
            Set<String> nullFields = new HashSet<>(remoteBibEntry.getFieldNames());
            nullFields.removeAll(localBibEntry.getFieldNames());
            for (String nullField : nullFields) {
                StringBuilder deleteFieldQuery = new StringBuilder();
                deleteFieldQuery.append("DELETE FROM ");
                deleteFieldQuery.append(escape("FIELD"));
                deleteFieldQuery.append(" WHERE ");
                deleteFieldQuery.append(escape("NAME"));
                deleteFieldQuery.append(" = ? AND ");
                deleteFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                deleteFieldQuery.append(" = ?");

                try (PreparedStatement preparedDeleteFieldStatement = dbmsHelper.prepareStatement(deleteFieldQuery.toString())) {
                    preparedDeleteFieldStatement.setString(1, nullField);
                    preparedDeleteFieldStatement.setInt(2, localBibEntry.getRemoteId());
                    preparedDeleteFieldStatement.executeUpdate();
                }
            }

            if (localBibEntry.getVersion() >= remoteBibEntry.getVersion()) {

                for (String fieldName : localBibEntry.getFieldNames()) {
                    // avoiding to use deprecated BibEntry.getField() method. null values are accepted by PreparedStatement!
                    Optional<String> valueOptional = localBibEntry.getFieldOptional(fieldName);
                    String value = null;
                    if (valueOptional.isPresent()) {
                        value = valueOptional.get();
                    }

                    StringBuilder selectFieldQuery = new StringBuilder();
                    selectFieldQuery.append("SELECT * FROM ");
                    selectFieldQuery.append(escape("FIELD"));
                    selectFieldQuery.append(" WHERE ");
                    selectFieldQuery.append(escape("NAME"));
                    selectFieldQuery.append(" = ? AND ");
                    selectFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                    selectFieldQuery.append(" = ?");

                    try (PreparedStatement preparedSelectFieldStatement = dbmsHelper.prepareStatement(selectFieldQuery.toString())) {
                        preparedSelectFieldStatement.setString(1, fieldName);
                        preparedSelectFieldStatement.setInt(2, localBibEntry.getRemoteId());

                        try (ResultSet selectFieldResultSet = preparedSelectFieldStatement.executeQuery()) {
                            if (selectFieldResultSet.next()) { // check if field already exists
                                StringBuilder updateFieldQuery = new StringBuilder();
                                updateFieldQuery.append("UPDATE ");
                                updateFieldQuery.append(escape("FIELD"));
                                updateFieldQuery.append(" SET ");
                                updateFieldQuery.append(escape("VALUE"));
                                updateFieldQuery.append(" = ? WHERE ");
                                updateFieldQuery.append(escape("NAME"));
                                updateFieldQuery.append(" = ? AND ");
                                updateFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                                updateFieldQuery.append(" = ?");

                                try (PreparedStatement preparedUpdateFieldStatement = dbmsHelper
                                        .prepareStatement(updateFieldQuery.toString())) {
                                    preparedUpdateFieldStatement.setString(1, value);
                                    preparedUpdateFieldStatement.setString(2, fieldName);
                                    preparedUpdateFieldStatement.setInt(3, localBibEntry.getRemoteId());
                                    preparedUpdateFieldStatement.executeUpdate();
                                }
                            } else {
                                StringBuilder insertFieldQuery = new StringBuilder();
                                insertFieldQuery.append("INSERT INTO ");
                                insertFieldQuery.append(escape("FIELD"));
                                insertFieldQuery.append("(");
                                insertFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                                insertFieldQuery.append(", ");
                                insertFieldQuery.append(escape("NAME"));
                                insertFieldQuery.append(", ");
                                insertFieldQuery.append(escape("VALUE"));
                                insertFieldQuery.append(") VALUES(?, ?, ?)");

                                try (PreparedStatement preparedFieldStatement = dbmsHelper
                                        .prepareStatement(insertFieldQuery.toString())) {
                                    preparedFieldStatement.setInt(1, localBibEntry.getRemoteId());
                                    preparedFieldStatement.setString(2, fieldName);
                                    preparedFieldStatement.setString(3, value);
                                    preparedFieldStatement.executeUpdate();
                                }
                            }
                        }
                    }
                }

                // updating entry type
                StringBuilder updateEntryTypeQuery = new StringBuilder();
                updateEntryTypeQuery.append("UPDATE ");
                updateEntryTypeQuery.append(escape("ENTRY"));
                updateEntryTypeQuery.append(" SET ");
                updateEntryTypeQuery.append(escape("TYPE"));
                updateEntryTypeQuery.append(" = ?, ");
                updateEntryTypeQuery.append(escape("VERSION"));
                updateEntryTypeQuery.append(" = ");
                updateEntryTypeQuery.append(escape("VERSION"));
                updateEntryTypeQuery.append(" + 1 WHERE ");
                updateEntryTypeQuery.append(escape("REMOTE_ID"));
                updateEntryTypeQuery.append(" = ?");
                try (PreparedStatement preparedUpdateEntryTypeStatement = dbmsHelper.prepareStatement(updateEntryTypeQuery.toString())) {
                    preparedUpdateEntryTypeStatement.setString(1, localBibEntry.getType());
                    preparedUpdateEntryTypeStatement.setInt(2, localBibEntry.getRemoteId());
                    preparedUpdateEntryTypeStatement.executeUpdate();
                }

                dbmsHelper.commit(); // apply all changes in current transaction

            } else {
                throw new OfflineLockException(localBibEntry, remoteBibEntry);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
            dbmsHelper.rollback(); // undo changes made in current transaction
        } finally {
            dbmsHelper.setAutoCommit(true); // enable auto commit mode again
        }
    }

    /**
     * Removes the remote existing bibEntry
     * @param bibEntry {@link BibEntry} to be deleted
     */
    public void removeEntry(BibEntry bibEntry) {
        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM ");
        query.append(escape("ENTRY"));
        query.append(" WHERE ");
        query.append(escape("REMOTE_ID"));
        query.append(" = ?");

        try (PreparedStatement preparedStatement = dbmsHelper.prepareStatement(query.toString())) {
            preparedStatement.setInt(1, bibEntry.getRemoteId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }

    }

    /**
     * @param remoteId Entry ID
     * @return instance of {@link BibEntry}
     */
    public Optional<BibEntry> getRemoteEntry(int remoteId) {
        List<BibEntry> remoteEntries = getRemoteEntryList(remoteId);
        if (!remoteEntries.isEmpty()) {
            return Optional.of(remoteEntries.get(0));
        }
        return Optional.empty();
    }

    public List<BibEntry> getRemoteEntries() {
        return getRemoteEntryList(0);
    }

    /**
     * @param remoteId Entry ID. If 0, all entries are going to be fetched.
     * @return List of {@link BibEntry} instances
     */
    private List<BibEntry> getRemoteEntryList(int remoteId) {
        List<BibEntry> remoteEntries = new ArrayList<>();

        StringBuilder selectEntryQuery = new StringBuilder();
        selectEntryQuery.append("SELECT * FROM ");
        selectEntryQuery.append(escape("ENTRY"));

        if (remoteId != 0) {
            selectEntryQuery.append(" WHERE ");
            selectEntryQuery.append(escape("REMOTE_ID"));
            selectEntryQuery.append(" = ");
            selectEntryQuery.append(remoteId);
        }

        selectEntryQuery.append(" ORDER BY ");
        selectEntryQuery.append(escape("REMOTE_ID"));

        try (ResultSet selectEntryResultSet = dbmsHelper.query(selectEntryQuery.toString())) {
            while (selectEntryResultSet.next()) {
                BibEntry bibEntry = new BibEntry();
                // setting the base attributes once
                bibEntry.setRemoteId(selectEntryResultSet.getInt("REMOTE_ID"));
                bibEntry.setType(selectEntryResultSet.getString("TYPE"));
                bibEntry.setVersion(selectEntryResultSet.getInt("VERSION"));

                StringBuilder selectFieldQuery = new StringBuilder();
                selectFieldQuery.append("SELECT * FROM ");
                selectFieldQuery.append(escape("FIELD"));
                selectFieldQuery.append(" WHERE ");
                selectFieldQuery.append(escape("ENTRY_REMOTE_ID"));
                selectFieldQuery.append(" = ");
                selectFieldQuery.append(selectEntryResultSet.getInt("REMOTE_ID"));

                try (ResultSet selectFieldResultSet = dbmsHelper.query(selectFieldQuery.toString())) {
                    while (selectFieldResultSet.next()) {
                        bibEntry.setField(selectFieldResultSet.getString("NAME"),
                                Optional.ofNullable(selectFieldResultSet.getString("VALUE")), EntryEventSource.REMOTE);
                    }
                }
                remoteEntries.add(bibEntry);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error", e);
        }

        return remoteEntries;
    }

    /**
     * Retrieves a mapping between the columns REMOTE_ID and VERSION.
     */
    public Map<Integer, Integer> getRemoteIdVersionMapping() {
        Map<Integer, Integer> remoteIdVersionMapping = new HashMap<>();
        StringBuilder selectEntryQuery = new StringBuilder();
        selectEntryQuery.append("SELECT * FROM ");
        selectEntryQuery.append(escape("ENTRY"));
        selectEntryQuery.append(" ORDER BY ");
        selectEntryQuery.append(escape("REMOTE_ID"));

        try (ResultSet selectEntryResultSet = dbmsHelper.query(selectEntryQuery.toString())) {
            while (selectEntryResultSet.next()) {
                remoteIdVersionMapping.put(selectEntryResultSet.getInt("REMOTE_ID"), selectEntryResultSet.getInt("VERSION"));
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error", e);
        }

        return remoteIdVersionMapping;
    }

    /**
     * Fetches and returns all remotely present meta data.
     */
    public Map<String, String> getRemoteMetaData() {
        Map<String, String> data = new HashMap<>();

        try (ResultSet resultSet = dbmsHelper.query("SELECT * FROM " + escape("METADATA"))) {
            while(resultSet.next()) {
                data.put(resultSet.getString("KEY"), resultSet.getString("VALUE"));
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error", e);
        }

        return data;
    }

    /**
     * Clears and sets all meta data remotely.
     * @param metaData JabRef meta data.
     */
    public void setRemoteMetaData(Map<String, String> data) {
        dbmsHelper.executeUpdate("TRUNCATE TABLE " + escape("METADATA")); // delete data all data from table

        for (Map.Entry<String, String> metaEntry : data.entrySet()) {

            StringBuilder query = new StringBuilder();
            query.append("INSERT INTO ");
            query.append(escape("METADATA"));
            query.append("(");
            query.append(escape("KEY"));
            query.append(", ");
            query.append(escape("VALUE"));
            query.append(") VALUES(?, ?)");

            try (PreparedStatement preparedStatement = dbmsHelper.prepareStatement(query.toString())) {
                preparedStatement.setString(1, metaEntry.getKey());
                preparedStatement.setString(2, metaEntry.getValue());
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                LOGGER.error("SQL Error: ", e);
            }
        }
    }

    /**
     * Helping method for SQL selection retrieving a {@link ResultSet}
     * @param remoteId Remote existent ID of {@link BibEntry}
     */
    private ResultSet selectFromEntryTable(int remoteId) throws SQLException {
        StringBuilder query = new StringBuilder();

        query.append("SELECT * FROM ");
        query.append(escape("ENTRY"));
        query.append(" WHERE ");
        query.append(escape("REMOTE_ID"));
        query.append(" = ");
        query.append(remoteId);

        return dbmsHelper.query(query.toString());
    }

    /**
     * Escapes parts of SQL expressions like table or field name to match the conventions
     * of the database system.
     * @param expression Table or field name
     * @param type Type of database system
     * @return Correctly escape expression
     */
    public static String escape(String expression, DBMSType type) {
        if (type == DBMSType.MYSQL) {
            return "`" + expression + "`";
        } else if ((type == DBMSType.ORACLE) || (type == DBMSType.POSTGRESQL)) {
            return "\"" + expression + "\"";
        }
        return expression;
    }

    /**
     * Escapes parts of SQL expressions like table or field name to match the conventions
     * of the database system using the current dbmsType.
     * @param expression Table or field name
     * @return Correctly escape expression
     */
    public String escape(String expression) {
        return escape(expression, dbmsType);
    }

    public void setDBType(DBMSType dbmsType) {
        this.dbmsType = dbmsType;
    }

    public DBMSType getDBType() {
        return this.dbmsType;
    }

}
