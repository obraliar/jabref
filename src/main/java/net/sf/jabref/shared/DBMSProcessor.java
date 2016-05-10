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
import net.sf.jabref.shared.exception.OfflineLockException;
import net.sf.jabref.shared.exception.SharedEntryNotPresentException;

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
    public void setUpSharedDatabase() {
        if (dbmsType == DBMSType.MYSQL) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS `ENTRY` (" +
                "`SHARED_ID` INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
                "`TYPE` VARCHAR(255) NOT NULL, " +
                "`VERSION` INT(11) DEFAULT 1)");

            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS `FIELD` (" +
                "`ENTRY_SHARED_ID` INT(11) NOT NULL, " +
                "`NAME` VARCHAR(255) NOT NULL, " +
                "`VALUE` TEXT DEFAULT NULL, " +
                "FOREIGN KEY (`ENTRY_SHARED_ID`) REFERENCES `ENTRY`(`SHARED_ID`) ON DELETE CASCADE)");

            dbmsHelper.executeUpdate("CREATE TABLE IF NOT EXISTS `METADATA` (" +
                    "`KEY` varchar(255) NOT NULL," +
                    "`VALUE` text NOT NULL)");

        } else if (dbmsType == DBMSType.POSTGRESQL) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"ENTRY\" (" +
                "\"SHARED_ID\" SERIAL PRIMARY KEY, " +
                "\"TYPE\" VARCHAR, " +
                "\"VERSION\" INTEGER DEFAULT 1)");

            dbmsHelper.executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"FIELD\" (" +
                "\"ENTRY_SHARED_ID\" INTEGER REFERENCES \"ENTRY\"(\"SHARED_ID\") ON DELETE CASCADE, " +
                "\"NAME\" VARCHAR, " +
                "\"VALUE\" TEXT)");

            dbmsHelper.executeUpdate("CREATE TABLE IF NOT EXISTS \"METADATA\" ("
                    + "\"KEY\" VARCHAR,"
                    + "\"VALUE\" TEXT)");

        } else if (dbmsType == DBMSType.ORACLE) {
            dbmsHelper.executeUpdate(
                "CREATE TABLE \"ENTRY\" (" +
                "\"SHARED_ID\" NUMBER NOT NULL, " +
                "\"TYPE\" VARCHAR2(255) NULL, " +
                "\"VERSION\" NUMBER DEFAULT 1, " +
                "CONSTRAINT \"ENTRY_PK\" PRIMARY KEY (\"SHARED_ID\"))");

            dbmsHelper.executeUpdate("CREATE SEQUENCE \"ENTRY_SEQ\"");

            dbmsHelper.executeUpdate("CREATE TRIGGER \"ENTRY_T\" BEFORE INSERT ON \"ENTRY\" " +
                "FOR EACH ROW BEGIN SELECT \"ENTRY_SEQ\".NEXTVAL INTO :NEW.shared_id FROM DUAL; END;");

            dbmsHelper.executeUpdate(
                "CREATE TABLE \"FIELD\" (" +
                "\"ENTRY_SHARED_ID\" NUMBER NOT NULL, " +
                "\"NAME\" VARCHAR2(255) NOT NULL, " +
                "\"VALUE\" CLOB NULL, " +
                "CONSTRAINT \"ENTRY_SHARED_ID_FK\" FOREIGN KEY (\"ENTRY_SHARED_ID\") " +
                "REFERENCES \"ENTRY\"(\"SHARED_ID\") ON DELETE CASCADE)");

            dbmsHelper.executeUpdate("CREATE TABLE \"METADATA\" (" +
                    "\"KEY\"  VARCHAR2(255) NULL," +
                    "\"VALUE\"  CLOB NOT NULL)");

        }
        if (!checkBaseIntegrity()) {
            // can only happen with users direct intervention on shared database
            LOGGER.error(Localization.lang("Corrupt_shared_database_structure."));
        }
    }

    /**
     * Inserts the given bibEntry into shared database.
     * @param bibEntry {@link BibEntry} to be inserted
     */
    public void insertEntry(BibEntry bibEntry) {

        // Check if already exists
        int sharedID = bibEntry.getSharedID();
        if (sharedID != -1) {
            try (ResultSet resultSet = selectFromEntryTable(sharedID)) {
                if (resultSet.next()) {
                    return;
                }
            } catch (SQLException e) {
                LOGGER.error("SQL Error: ", e);
            }
        }

        // Inserting into ENTRY table
        StringBuilder insertIntoEntryQuery = new StringBuilder()
        .append("INSERT INTO ")
        .append(escape("ENTRY"))
        .append("(")
        .append(escape("TYPE"))
        .append(") VALUES(?)");

        // This is the only method to get generated keys which is accepted by MySQL, PostgreSQL and Oracle.
        try (PreparedStatement preparedEntryStatement = dbmsHelper.prepareStatement(insertIntoEntryQuery.toString(),
                "SHARED_ID")) {

            preparedEntryStatement.setString(1, bibEntry.getType());
            preparedEntryStatement.executeUpdate();

            try (ResultSet generatedKeys = preparedEntryStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.setSharedID(generatedKeys.getInt(1)); // set generated ID locally
                }
            }

            // Inserting into FIELD table
            for (String fieldName : bibEntry.getFieldNames()) {
                StringBuilder insertFieldQuery = new StringBuilder()
                .append("INSERT INTO ")
                .append(escape("FIELD"))
                .append("(")
                .append(escape("ENTRY_SHARED_ID"))
                .append(", ")
                .append(escape("NAME"))
                .append(", ")
                .append(escape("VALUE"))
                .append(") VALUES(?, ?, ?)");

                try (PreparedStatement preparedFieldStatement = dbmsHelper.prepareStatement(insertFieldQuery.toString())) {
                    // columnIndex starts with 1
                    preparedFieldStatement.setInt(1, bibEntry.getSharedID());
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
     * Updates the whole {@link BibEntry} on shared database.
     *
     * @param localBibEntry {@link BibEntry} affected by changes
     */
    public void updateEntry(BibEntry localBibEntry) throws OfflineLockException, SharedEntryNotPresentException {
        dbmsHelper.setAutoCommit(false); // disable auto commit due to transaction

        try {
            Optional<BibEntry> sharedEntryOptional = getSharedEntry(localBibEntry.getSharedID());

            if (!sharedEntryOptional.isPresent()) {
                throw new SharedEntryNotPresentException(localBibEntry);
            }

            BibEntry sharedBibEntry = sharedEntryOptional.get();

            // remove shared fields which does not exist locally
            Set<String> nullFields = new HashSet<>(sharedBibEntry.getFieldNames());
            nullFields.removeAll(localBibEntry.getFieldNames());
            for (String nullField : nullFields) {
                StringBuilder deleteFieldQuery = new StringBuilder()
                .append("DELETE FROM ")
                .append(escape("FIELD"))
                .append(" WHERE ")
                .append(escape("NAME"))
                .append(" = ? AND ")
                .append(escape("ENTRY_SHARED_ID"))
                .append(" = ?");

                try (PreparedStatement preparedDeleteFieldStatement = dbmsHelper.prepareStatement(deleteFieldQuery.toString())) {
                    preparedDeleteFieldStatement.setString(1, nullField);
                    preparedDeleteFieldStatement.setInt(2, localBibEntry.getSharedID());
                    preparedDeleteFieldStatement.executeUpdate();
                }
            }

            if (localBibEntry.getVersion() >= sharedBibEntry.getVersion()) {

                for (String fieldName : localBibEntry.getFieldNames()) {
                    // avoiding to use deprecated BibEntry.getField() method. null values are accepted by PreparedStatement!
                    Optional<String> valueOptional = localBibEntry.getFieldOptional(fieldName);
                    String value = null;
                    if (valueOptional.isPresent()) {
                        value = valueOptional.get();
                    }

                    StringBuilder selectFieldQuery = new StringBuilder()
                    .append("SELECT * FROM ")
                    .append(escape("FIELD"))
                    .append(" WHERE ")
                    .append(escape("NAME"))
                    .append(" = ? AND ")
                    .append(escape("ENTRY_SHARED_ID"))
                    .append(" = ?");

                    try (PreparedStatement preparedSelectFieldStatement = dbmsHelper.prepareStatement(selectFieldQuery.toString())) {
                        preparedSelectFieldStatement.setString(1, fieldName);
                        preparedSelectFieldStatement.setInt(2, localBibEntry.getSharedID());

                        try (ResultSet selectFieldResultSet = preparedSelectFieldStatement.executeQuery()) {
                            if (selectFieldResultSet.next()) { // check if field already exists
                                StringBuilder updateFieldQuery = new StringBuilder()
                                .append("UPDATE ")
                                .append(escape("FIELD"))
                                .append(" SET ")
                                .append(escape("VALUE"))
                                .append(" = ? WHERE ")
                                .append(escape("NAME"))
                                .append(" = ? AND ")
                                .append(escape("ENTRY_SHARED_ID"))
                                .append(" = ?");

                                try (PreparedStatement preparedUpdateFieldStatement = dbmsHelper
                                        .prepareStatement(updateFieldQuery.toString())) {
                                    preparedUpdateFieldStatement.setString(1, value);
                                    preparedUpdateFieldStatement.setString(2, fieldName);
                                    preparedUpdateFieldStatement.setInt(3, localBibEntry.getSharedID());
                                    preparedUpdateFieldStatement.executeUpdate();
                                }
                            } else {
                                StringBuilder insertFieldQuery = new StringBuilder()
                                .append("INSERT INTO ")
                                .append(escape("FIELD"))
                                .append("(")
                                .append(escape("ENTRY_SHARED_ID"))
                                .append(", ")
                                .append(escape("NAME"))
                                .append(", ")
                                .append(escape("VALUE"))
                                .append(") VALUES(?, ?, ?)");

                                try (PreparedStatement preparedFieldStatement = dbmsHelper
                                        .prepareStatement(insertFieldQuery.toString())) {
                                    preparedFieldStatement.setInt(1, localBibEntry.getSharedID());
                                    preparedFieldStatement.setString(2, fieldName);
                                    preparedFieldStatement.setString(3, value);
                                    preparedFieldStatement.executeUpdate();
                                }
                            }
                        }
                    }
                }

                // updating entry type
                StringBuilder updateEntryTypeQuery = new StringBuilder()
                .append("UPDATE ")
                .append(escape("ENTRY"))
                .append(" SET ")
                .append(escape("TYPE"))
                .append(" = ?, ")
                .append(escape("VERSION"))
                .append(" = ")
                .append(escape("VERSION"))
                .append(" + 1 WHERE ")
                .append(escape("SHARED_ID"))
                .append(" = ?");
                try (PreparedStatement preparedUpdateEntryTypeStatement = dbmsHelper.prepareStatement(updateEntryTypeQuery.toString())) {
                    preparedUpdateEntryTypeStatement.setString(1, localBibEntry.getType());
                    preparedUpdateEntryTypeStatement.setInt(2, localBibEntry.getSharedID());
                    preparedUpdateEntryTypeStatement.executeUpdate();
                }

                dbmsHelper.commit(); // apply all changes in current transaction

            } else {
                throw new OfflineLockException(localBibEntry, sharedBibEntry);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
            dbmsHelper.rollback(); // undo changes made in current transaction
        } finally {
            dbmsHelper.setAutoCommit(true); // enable auto commit mode again
        }
    }

    /**
     * Removes the shared bibEntry.
     * @param bibEntry {@link BibEntry} to be deleted
     */
    public void removeEntry(BibEntry bibEntry) {
        StringBuilder query = new StringBuilder()
        .append("DELETE FROM ")
        .append(escape("ENTRY"))
        .append(" WHERE ")
        .append(escape("SHARED_ID"))
        .append(" = ?");

        try (PreparedStatement preparedStatement = dbmsHelper.prepareStatement(query.toString())) {
            preparedStatement.setInt(1, bibEntry.getSharedID());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }

    }

    /**
     * @param sharedID Entry ID
     * @return instance of {@link BibEntry}
     */
    public Optional<BibEntry> getSharedEntry(int sharedID) {
        List<BibEntry> sharedEntries = getSharedEntryList(sharedID);
        if (!sharedEntries.isEmpty()) {
            return Optional.of(sharedEntries.get(0));
        }
        return Optional.empty();
    }

    public List<BibEntry> getSharedEntries() {
        return getSharedEntryList(0);
    }

    /**
     * @param sharedID Entry ID. If 0, all entries are going to be fetched.
     * @return List of {@link BibEntry} instances
     */
    private List<BibEntry> getSharedEntryList(int sharedID) {
        List<BibEntry> sharedEntries = new ArrayList<>();

        StringBuilder selectEntryQuery = new StringBuilder();
        selectEntryQuery.append("SELECT * FROM ");
        selectEntryQuery.append(escape("ENTRY"));

        if (sharedID != 0) {
            selectEntryQuery.append(" WHERE ");
            selectEntryQuery.append(escape("SHARED_ID"));
            selectEntryQuery.append(" = ");
            selectEntryQuery.append(sharedID);
        }

        selectEntryQuery.append(" ORDER BY ");
        selectEntryQuery.append(escape("SHARED_ID"));

        try (ResultSet selectEntryResultSet = dbmsHelper.query(selectEntryQuery.toString())) {
            while (selectEntryResultSet.next()) {
                BibEntry bibEntry = new BibEntry();
                // setting the base attributes once
                bibEntry.setSharedID(selectEntryResultSet.getInt("SHARED_ID"));
                bibEntry.setType(selectEntryResultSet.getString("TYPE"));
                bibEntry.setVersion(selectEntryResultSet.getInt("VERSION"));

                StringBuilder selectFieldQuery = new StringBuilder()
                .append("SELECT * FROM ")
                .append(escape("FIELD"))
                .append(" WHERE ")
                .append(escape("ENTRY_SHARED_ID"))
                .append(" = ")
                .append(selectEntryResultSet.getInt("SHARED_ID"));

                try (ResultSet selectFieldResultSet = dbmsHelper.query(selectFieldQuery.toString())) {
                    while (selectFieldResultSet.next()) {
                        bibEntry.setField(selectFieldResultSet.getString("NAME"),
                                Optional.ofNullable(selectFieldResultSet.getString("VALUE")), EntryEventSource.SHARED);
                    }
                }
                sharedEntries.add(bibEntry);
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error", e);
        }

        return sharedEntries;
    }

    /**
     * Retrieves a mapping between the columns SHARED_ID and VERSION.
     */
    public Map<Integer, Integer> getSharedIDVersionMapping() {
        Map<Integer, Integer> sharedIDVersionMapping = new HashMap<>();
        StringBuilder selectEntryQuery = new StringBuilder()
        .append("SELECT * FROM ")
        .append(escape("ENTRY"))
        .append(" ORDER BY ")
        .append(escape("SHARED_ID"));

        try (ResultSet selectEntryResultSet = dbmsHelper.query(selectEntryQuery.toString())) {
            while (selectEntryResultSet.next()) {
                sharedIDVersionMapping.put(selectEntryResultSet.getInt("SHARED_ID"), selectEntryResultSet.getInt("VERSION"));
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error", e);
        }

        return sharedIDVersionMapping;
    }

    /**
     * Fetches and returns all shared meta data.
     */
    public Map<String, String> getSharedMetaData() {
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
     * Clears and sets all shared meta data.
     * @param metaData JabRef meta data.
     */
    public void setSharedMetaData(Map<String, String> data) {
        dbmsHelper.executeUpdate("TRUNCATE TABLE " + escape("METADATA")); // delete data all data from table

        for (Map.Entry<String, String> metaEntry : data.entrySet()) {

            StringBuilder query = new StringBuilder()
            .append("INSERT INTO ")
            .append(escape("METADATA"))
            .append("(")
            .append(escape("KEY"))
            .append(", ")
            .append(escape("VALUE"))
            .append(") VALUES(?, ?)");

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
     * @param sharedID Existent ID of {@link BibEntry} on shared database
     */
    private ResultSet selectFromEntryTable(int sharedID) throws SQLException {
        StringBuilder query = new StringBuilder()
        .append("SELECT * FROM ")
        .append(escape("ENTRY"))
        .append(" WHERE ")
        .append(escape("SHARED_ID"))
        .append(" = ")
        .append(sharedID);

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
