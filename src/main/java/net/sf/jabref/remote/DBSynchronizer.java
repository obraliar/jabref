package net.sf.jabref.remote;

import java.sql.Connection;
import java.util.ArrayList;

import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private Connection connection;
    private final DBHelper dbHelper = new DBHelper();
    private DBType dbType;


    @Subscribe
    public void listen(EntryAddedEvent event) {
        //System.out.println("EntryAddedEvent " + this.toString());

        /*if (dbHelper.checkIntegrity(DBType.MYSQL)) {
            System.out.println("checkIntegrity: OK.");
        } else {
            System.out.println("checkIntegrity: NOT OK.");
        }*/

        dbHelper.insertEntry(event.getBibEntry());

    }

    /*@Subscribe
    public void listen(EntryChangedEvent event) {
        //System.out.println("EntryChangedEvent " + this.toString());

        dbHelper.updateEntry(event.getBibEntry());

    }*/

    @Subscribe
    public void listen(FieldChangedEvent event) {
        //System.out.println("FieldChangedEvent " + this.toString());

        dbHelper.updateEntry(event.getBibEntry(), event.getFieldName(), event.getNewValue());
    }

    @Subscribe
    public void listen(EntryRemovedEvent event) {
        //System.out.println("EntryRemovedEvent " + this.toString());
        dbHelper.removeEntry(event.getBibEntry());
    }

    // todo synchronizer: ignore fields jabref_database, type ...
    //TODO improve!!!
    public void synchronizeLocalDatabase(BibDatabase bibDatabase) {

        if (!dbHelper.checkIntegrity()) {
            System.out.println("checkIntegrity: NOT OK. Fixing...");
            dbHelper.setUpRemoteDatabase(this.dbType);
        }

        try {
            ArrayList<ArrayList<String>> matrix = dbHelper.getQueryResultMatrix("SELECT * FROM entry");

            ArrayList<String> columns = matrix.get(0);

            for (int i = 1; i < matrix.size(); i++) {
                ArrayList<String> row = matrix.get(i);
                BibEntry bibEntry = new BibEntry();
                for (int j = 0; j < row.size(); j++) {
                    String value = row.get(j);
                    if (value != null) {
                        if (columns.get(j).equals("remote_id")) {
                            Integer remote_id = Integer.parseInt(value);
                            bibEntry.setRemoteId(remote_id);
                        } else if (columns.get(j).equals("entrytype")) {
                            bibEntry.setType(value);
                        } else {
                            bibEntry.setField(columns.get(j), value);
                        }
                    }
                }
                bibDatabase.insertEntry(bibEntry);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*public void synchronizeLocalDatabase2(BibDatabase bibDatabase) {
        try (ResultSet resultSet = dbHelper.query("SELECT * FROM entry")) {
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
    }*/

    public void setConnection(Connection connection) {
        this.connection = connection;
        this.dbHelper.setConnection(connection);
    }

    // TODO getter: database name (probably not the right place)
    public String getRemoteDatabaseName() {
        return "test123";
    }

    public void setDBType(DBType dbType) {
        this.dbType = dbType;
        dbHelper.setDBType(dbType);
    }

    public DBType getDBType() {
        return this.dbType;
    }

}
