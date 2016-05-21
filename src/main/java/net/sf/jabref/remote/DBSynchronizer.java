package net.sf.jabref.remote;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private DBProcessor dbProcessor;
    private DBType dbType;
    private String dbName;


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

    public void synchronizeLocalDatabase(BibDatabase bibDatabase) {
        List<BibEntry> localEntries = bibDatabase.getEntries();
        List<BibEntry> remoteEntries = dbProcessor.getRemoteEntries();

        for (BibEntry remoteEntry : remoteEntries) {
            boolean match = false;
            for (BibEntry localEntry : localEntries) {
                if (remoteEntry.getRemoteId() == localEntry.getRemoteId()) {
                    match = true;
                    Set<String> fields = remoteEntry.getFieldNames();
                    for (String field : fields) {
                        localEntry.setField(field, remoteEntry.getField(field));
                    }
                }
            }
            if (!match) {
                bibDatabase.insertEntry(remoteEntry);
            }
        }
    }

    public String getDBName() {
        return dbName;
    }

    public DBType getDBType() {
        return this.dbType;
    }

    public void setUp(Connection connection, DBType dbType, String dbName) {
        this.dbType = dbType;
        this.dbName = dbName;
        this.dbProcessor = new DBProcessor(connection, dbType);
    }

}
