package net.sf.jabref.remote;

import java.sql.Connection;
import java.util.ArrayList;

import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryChangedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private Connection connection;
    private final DBHelper dbHelper = new DBHelper();


    @Subscribe
    public void listen(EntryAddedEvent event) {
        System.out.println("EntryAddedEvent " + this.toString());

        if (dbHelper.checkIntegrity(DBType.MYSQL)) {
            System.out.println("checkIntegrity: OK.");
        } else {
            System.out.println("checkIntegrity: NOT OK.");
        }
        dbHelper.insertNewEntry(event.getBibEntry());

    }

    @Subscribe
    public void listen(EntryChangedEvent event) {
        System.out.println("EntryChangedEvent " + this.toString());

        dbHelper.updateEntry(event.getBibEntry());

    }

    @Subscribe
    public void listen(EntryRemovedEvent event) {
        System.out.println("EntryRemovedEvent " + this.toString());
    }

    @Subscribe
    public void listen(FieldChangedEvent event) {
        System.out.println("FieldChangedEvent " + this.toString());
    }

    public void synchronizeLocalDatabase(BibDatabase bibDatabase) {

        try {
            ArrayList<ArrayList<String>> matrix = dbHelper.getQueryResultMatrix("SELECT * FROM entry");

            ArrayList<String> columns = matrix.get(0);

            for (int i = 1; i < matrix.size(); i++) {
                ArrayList<String> row = matrix.get(i);
                BibEntry bibEntry = new BibEntry();
                for (int j = 0; j < row.size(); j++) {
                    String value = row.get(j);
                    if (value != null) {
                        bibEntry.setField(columns.get(j), value);
                    }
                }
                bibEntry.setType("article"); //TODO
                bibDatabase.insertEntry(bibEntry);

                System.out.println(bibEntry.hashCode());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void setConnection(Connection connection) {
        this.connection = connection;
        this.dbHelper.setConnection(connection);
    }

}
