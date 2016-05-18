package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import net.sf.jabref.model.entry.BibEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// TODO Locking
// TODO Exceptions

public class DBHelper {

    private static final Log LOGGER = LogFactory.getLog(DBConnector.class);

    private Connection connection;


    public boolean checkIntegrity(DBType dbType) {
        if (dbType == DBType.MYSQL) {
            try (ResultSet resultSet = query("SELECT * FROM entry")) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                return resultSetMetaData.getColumnName(1).equals("remote_id")
                        && resultSetMetaData.getColumnTypeName(1).equals("INT");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
        /*... TODO ... also for other types*/
    }

    public void insertEntry(BibEntry bibEntry) {
        prepareEntryTableStructure(bibEntry);

        // Check if exists
        int remote_id = bibEntry.getRemoteId();
        if (remote_id != -1) {
            try (ResultSet resultSet = query("SELECT * FROM entry WHERE remote_id = " + remote_id)) {
                if (resultSet.next()) {
                    System.out.println("/!\\ WARNING: already exists."); //TODO warning on gui (exclude case when entries are imported at the beginning)
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
            query = query + keyList.get(i);
            query = i == (keyList.size() - 1) ? query : query + ", ";
        }
        query = query + ") VALUES(";
        for (int i = 0; i < keyList.size(); i++) {
            query = query + "\"" + bibEntry.getField(keyList.get(i)) + "\"";
            query = i == (keyList.size() - 1) ? query : query + ", ";
        }
        query = query + ")";


        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                bibEntry.setRemoteId(generatedKeys.getInt(1));
            }
            statement.close();
            generatedKeys.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        System.out.println(">>> SQL INSERT: " + query);

    }


    public void updateEntry(BibEntry bibEntry, String column, String newValue) {
        prepareEntryTableStructure(bibEntry);
        System.out.println(">>> SQL UPDATE: " + "UPDATE entry SET " + column + " = " + "\""+ newValue  +"\" WHERE remote_id = " + bibEntry.getRemoteId());
        try {
            connection.createStatement().executeUpdate("UPDATE entry SET " + column + " = " + "\""+ newValue  +"\" WHERE remote_id = " + bibEntry.getRemoteId());
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

    /*public void updateEntry(BibEntry bibEntry) {
    prepareEntryTableStructure(bibEntry);

    // TODO Case when bibEntry was not synchronized and remote_id is -1
    String query = "UPDATE entry SET ";
    ArrayList<String> columnNameList = new ArrayList<>();
    columnNameList.addAll(getColumnNames());
    columnNameList.remove("remote_id");

    for (int i = 0; i < columnNameList.size(); i++) {
        String columnName = columnNameList.get(i);
        if (bibEntry.getField(columnName) != null) {
            query = query + columnName + " = \"" + bibEntry.getField(columnName) + "\"";
        } else {
            query = query + columnName + " = NULL";
        }
        query = i < (columnNameList.size() - 1) ? query + ", " : query;
    }

    query = query + " WHERE remote_id = " + bibEntry.getRemoteId();
    System.out.println(">>> SQL UPDATE: " + query);

    try {
        connection.createStatement().executeUpdate(query);
    } catch (SQLException e) {
        e.printStackTrace();
    }
    }*/
}
