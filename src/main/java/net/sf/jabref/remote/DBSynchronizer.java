package net.sf.jabref.remote;

import java.sql.Connection;
import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private DBProcessor dbProcessor;
    private DBType dbType;


    @Subscribe
    public void listen(EntryAddedEvent event) {
        dbProcessor.insertEntry(event.getBibEntry());
    }

    @Subscribe
    public void listen(FieldChangedEvent event) {
        dbProcessor.updateEntry(event.getBibEntry(), event.getFieldName(), event.getNewValue());
    }

    @Subscribe
    public void listen(EntryRemovedEvent event) {
        dbProcessor.removeEntry(event.getBibEntry());
    }

    public void initializeLocalDatabase(BibDatabase bibDatabase) {

        if (!dbProcessor.checkIntegrity()) {
            System.out.println("Integrity check failed. Fixing...");
            dbProcessor.setUpRemoteDatabase();
        }

        for (BibEntry bibEntry : dbProcessor.getRemoteEntries()) {
            bibDatabase.insertEntry(bibEntry);
        }

    }

    // TODO getter: database name (probably not the right place)
    public String getRemoteDatabaseName() {
        return "test123";
    }

    public DBType getDBType() {
        return this.dbType;
    }

    public void setUp(Connection connection, DBType dbType) {
        this.dbType = dbType;
        this.dbProcessor = new DBProcessor(connection, dbType);
    }

}
