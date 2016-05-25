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

/**
 * Synchronizes the remote or local databases with their opposite side.
 * Local changes are pushed by {@link EntryEvent} using Google's Guava EventBus.
 */
public class DBSynchronizer {

    private DBProcessor dbProcessor;
    private DBType dbType;
    private String dbName;


    /**
     * Listening method. Inserts a new {@link BibEntry} remotely.
     * @param event {@link EntryAddedEvent} object
     */
    @Subscribe
    public void listen(EntryAddedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to insert the bibEntry entry again (but it would not harm).
        if (isInEventLocation(event)) {
            dbProcessor.insertEntry(event.getBibEntry());
        }
    }

    /**
     * Listening method. Updates an existing remote {@link BibEntry}.
     * @param event {@link FieldChangedEvent} object
     */
    @Subscribe
    public void listen(FieldChangedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to update the bibEntry entry again (but it would not harm).
        if (isInEventLocation(event)) {
            dbProcessor.updateEntry(event.getBibEntry(), event.getFieldName(), event.getNewValue());
        }
    }

    /**
     * Listening method. Deletes the given {@link BibEntry} remotely.
     * @param event {@link EntryRemovedEvent} object
     */
    @Subscribe
    public void listen(EntryRemovedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to delete the bibEntry entry again (but it would not harm).
        if (isInEventLocation(event)) {
            dbProcessor.removeEntry(event.getBibEntry());
        }
    }

    /**
     * Sets the remote table structure if needed and pulls all remote entries
     * to the local database.
     * @param bibDatabase Local {@link BibDatabase}
     */
    public void initializeLocalDatabase(BibDatabase bibDatabase) {

        if (!dbProcessor.checkIntegrity()) {
            System.out.println("Integrity check failed. Fixing...");
            dbProcessor.setUpRemoteDatabase();
        }

        for (BibEntry bibEntry : dbProcessor.getRemoteEntries()) {
            bibDatabase.insertEntry(bibEntry);
        }

    }

    /**
     * Synchronizes the local database with a remote one.
     * Possible update types are removal, update or insert of a {@link BibEntry}.
     * @param bibDatabase {@link BibDatabase} to be synchronized
     */
    public void synchronizeLocalDatabase(BibDatabase bibDatabase) {
        dbProcessor.normalizeEntryTable(); // remove unused columns

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
                bibDatabase.removeEntry(localEntry, EntryEventLocation.LOCAL); // Should not reach the listeners above.
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
                        localEntry.setField(field, remoteEntry.getField(field), EntryEventLocation.LOCAL); // Should not reach the listeners above.
                    }
                }
            }
            if (!match) {
                bibDatabase.insertEntry(remoteEntry, EntryEventLocation.LOCAL); // Should not reach the listeners above.
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
