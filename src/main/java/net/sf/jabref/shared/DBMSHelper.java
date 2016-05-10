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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains some helping methods related to the external SQL database.
 */
public class DBMSHelper {

    private static final Log LOGGER = LogFactory.getLog(DBMSConnector.class);

    private final Connection connection;


    public DBMSHelper(Connection connection) {
        this.connection = connection;
    }

    /**
     * Executes the given query and retrieves the {@link ResultSet}
     * @param query SQL Query
     * @return Instance of {@link ResultSet}
     */
    public ResultSet query(String query) throws SQLException {
        return connection.createStatement().executeQuery(query);
    }

    /**
     * Executes the given query as SQL update
     * @param query SQL Query
     */
    public void executeUpdate(String query) {
        try {
            connection.createStatement().executeUpdate(query);
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    /**
     * @return {@link DatabaseMetaData} of the current {@link Connection}
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    /**
     * @param query SQL query
     * @return Instance of {@link PreparedStatement}
     */
    public PreparedStatement prepareStatement(String query) throws SQLException {
        return connection.prepareStatement(query);
    }

    /**
     * @param query SQL query
     * @param columnNames Column names which should be returned
     * @return Instance of {@link PreparedStatement}
     */
    public PreparedStatement prepareStatement(String query, String... columnNames) throws SQLException {
        return connection.prepareStatement(query, columnNames);
    }

    /**
     * Sets whether the SQL queries should be committed automatically or not.
     * See <code>setsetAutoCommit(...)</code> in {@link Connection}
     */
    public void setAutoCommit(boolean autoCommit) {
        try {
            this.connection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    /**
     * Commit and execute all remaining SQL queries.
     * See <code>commit()</code> in {@link Connection}
     */
    public void commit() {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    /**
     * Undoes made commit and rolls the database back to the previous state.
     * See <code>rollback()</code> in {@link Connection}
     */
    public void rollback() {
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

}
