package net.sf.jabref.remote;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.event.location.EntryEventLocation;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private DBProcessor dbProcessor;
    private DBType dbType;
    private String dbName;


    @Subscribe
    public void listen(EntryAddedEvent event) {
        if (isInEventLocation(event)) {
            dbProcessor.insertEntry(event.getBibEntry());
        }
    }

    @Subscribe
    public void listen(FieldChangedEvent event) {
        if (isInEventLocation(event)) {
            dbProcessor.updateEntry(event.getBibEntry(), event.getFieldName(), event.getNewValue());
        }
    }

    @Subscribe
    public void listen(EntryRemovedEvent event) {
        if (isInEventLocation(event)) {
            dbProcessor.removeEntry(event.getBibEntry());
        }
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

        for (int i = 0; i < localEntries.size(); i++) {
            BibEntry localEntry = localEntries.get(i);
            boolean match = false;
            for (int j = 0; j < remoteEntries.size(); j++) {
                if (localEntry.getRemoteId() == remoteEntries.get(j).getRemoteId()) {
                    match = true;
                }
            }
            if (!match) {
                bibDatabase.removeEntry(localEntry, EntryEventLocation.LOCAL);
            }
        }

        for (int i = 0; i < remoteEntries.size(); i++) {
            BibEntry remoteEntry = remoteEntries.get(i);
            boolean match = false;
            for (int j = 0; j < localEntries.size(); j++) {
                BibEntry localEntry = localEntries.get(j);
                if (remoteEntry.getRemoteId() == localEntry.getRemoteId()) {
                    match = true;
                    Set<String> fields = remoteEntry.getFieldNames();
                    for (String field : fields) {
                        localEntry.setField(field, remoteEntry.getField(field), EntryEventLocation.LOCAL);
                    }
                }
            }
            if (!match) {
                bibDatabase.insertEntry(remoteEntry, EntryEventLocation.LOCAL);
            }
        }
    }


    public String getDBName() {
        return dbName;
    }

    public DBType getDBType() {
        return this.dbType;
    }

    public boolean isInEventLocation(EntryEvent event) {
        EntryEventLocation eventLocation = event.getEntryEventLocation();
        return ((eventLocation == EntryEventLocation.REMOTE) || (eventLocation == EntryEventLocation.ALL));
    }

    public void setUp(Connection connection, DBType dbType, String dbName) {
        this.dbType = dbType;
        this.dbName = dbName;
        this.dbProcessor = new DBProcessor(connection, dbType);
    }

}
