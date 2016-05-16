package net.sf.jabref.remote;

import java.sql.Connection;
import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryChangedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private Connection connection;


    @Subscribe
    public void listen(EntryAddedEvent event) {
        System.out.println("EntryAddedEvent " + this.toString());
    }

    @Subscribe
    public void listen(EntryChangedEvent event) {
        System.out.println("EntryChangedEvent " + this.toString());
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

        /*for (int i = 0; i < 10; i++) {
            BibEntry bibEntry = new BibEntry();
            bibEntry.setId("test" + i);
            bibEntry.setCiteKey("test" + i);
            bibDatabase.insertEntry(bibEntry);
        }*/
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

}
