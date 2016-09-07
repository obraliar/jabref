package net.sf.jabref.shared;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.jabref.model.entry.BibEntry;

import com.impossibl.postgres.jdbc.ThreadedHousekeeper;

/**
 * Processes all incoming or outgoing bib data to PostgreSQL database and manages its structure.
 */
public class PostgreSQLProcessor extends DBMSProcessor {

    /**
     * @param connection Working SQL connection
     * @param dbmsType Instance of {@link DBMSType}
     */
    public PostgreSQLProcessor(Connection connection) {
        super(connection);
        // Disable cleanup output of ThreadedHousekeeper
        Logger.getLogger(ThreadedHousekeeper.class.getName()).setLevel(Level.SEVERE);
    }

    /**
     * Creates and sets up the needed tables and columns according to the database type.
     *
     * @throws SQLException
     */
    @Override
    public void setUp() throws SQLException {
        connection.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"ENTRY\" (" +
                "\"SHARED_ID\" SERIAL PRIMARY KEY, " +
                "\"TYPE\" VARCHAR, " +
                "\"VERSION\" INTEGER DEFAULT 1)");

        connection.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"FIELD\" (" +
                "\"ENTRY_SHARED_ID\" INTEGER REFERENCES \"ENTRY\"(\"SHARED_ID\") ON DELETE CASCADE, " +
                "\"NAME\" VARCHAR, " +
                "\"VALUE\" TEXT)");

        connection.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS \"METADATA\" ("
                + "\"KEY\" VARCHAR,"
                + "\"VALUE\" TEXT)");
    }

    @Override
    protected void insertIntoEntryTable(BibEntry bibEntry) {
        // Inserting into ENTRY table
        StringBuilder insertIntoEntryQuery = new StringBuilder().append("INSERT INTO ").append(escape("ENTRY"))
                .append("(").append(escape("TYPE")).append(") VALUES(?)");

        // This is the only method to get generated keys which is accepted by MySQL, PostgreSQL and Oracle.
        try (PreparedStatement preparedEntryStatement = connection.prepareStatement(insertIntoEntryQuery.toString(),
                Statement.RETURN_GENERATED_KEYS)) {

            preparedEntryStatement.setString(1, bibEntry.getType());
            preparedEntryStatement.executeUpdate();

            try (ResultSet generatedKeys = preparedEntryStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    bibEntry.getSharedBibEntryData().setSharedID(generatedKeys.getInt(1)); // set generated ID locally
                }
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    @Override
    public String escape(String expression) {
        return "\"" + expression + "\"";
    }
}
