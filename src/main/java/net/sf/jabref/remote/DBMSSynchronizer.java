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
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.MetaData;
import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.event.MetaDataChangedEvent;
import net.sf.jabref.event.RemoteConnectionLostEvent;
import net.sf.jabref.event.source.EntryEventSource;
import net.sf.jabref.exporter.BibDatabaseWriter;
import net.sf.jabref.importer.fileformat.ParseException;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Synchronizes the remote or local databases with their opposite side.
 * Local changes are pushed by {@link EntryEvent} using Google's Guava EventBus.
 */
public class DBMSSynchronizer {

    private static final Log LOGGER = LogFactory.getLog(DBMSConnector.class);

    private DBMSProcessor dbmsProcessor;
    private DBMSType dbmsType;
    private String dbName;
    private final BibDatabaseContext bibDatabaseContext;
    private MetaData metaData;
    private final BibDatabase bibDatabase;
    private final EventBus eventBus;
    private Connection currentConnection;


    public DBMSSynchronizer(BibDatabaseContext bibDatabaseContext) {
        this.bibDatabaseContext = bibDatabaseContext;
        this.bibDatabase = bibDatabaseContext.getDatabase();
        this.metaData = bibDatabaseContext.getMetaData();
        this.eventBus = new EventBus();
    }

    /**
     * Listening method. Inserts a new {@link BibEntry} remotely.
     * @param event {@link EntryAddedEvent} object
     */
    @Subscribe
    public void listen(EntryAddedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to insert the bibEntry entry again (but it would not harm).
        if (isInEventLocation(event) && checkCurrentConnection()) {
            dbmsProcessor.insertEntry(event.getBibEntry());
            synchronizeLocalMetaData();
            synchronizeLocalDatabase(); // Pull remote changes for the case that there where some
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
        if (isInEventLocation(event) && checkCurrentConnection()) {
            synchronizeLocalMetaData();
            BibDatabaseWriter.applySaveActions(event.getBibEntry(), metaData);
            dbmsProcessor.updateEntry(event.getBibEntry());
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
        if (isInEventLocation(event) && checkCurrentConnection()) {
            dbmsProcessor.removeEntry(event.getBibEntry());
            synchronizeLocalMetaData();
            synchronizeLocalDatabase(); // Pull remote changes for the case that there where some
        }
    }

    /**
     * Listening method. Synchronizes the {@link MetaData} remotely and applies it locally.
     * @param event
     */
    @Subscribe
    public void listen(MetaDataChangedEvent event) {
        if (checkCurrentConnection()) {
            synchronizeRemoteMetaData(event.getMetaData());
            applyMetaData();
        }
    }

    /**
     * Sets the remote table structure if needed and pulls all remote entries
     * to the new local database.
     * @param bibDatabase Local {@link BibDatabase}
     */
    public void initializeDatabases() {

        if (!dbmsProcessor.checkBaseIntegrity()) {
            LOGGER.info(Localization.lang("Integrity check failed. Fixing..."));
            dbmsProcessor.setUpRemoteDatabase();
        }
        synchronizeLocalMetaData();
        synchronizeLocalDatabase();
    }

    /**
     * Synchronizes the local database with a remote one.
     * Possible update types are removal, update or insert of a {@link BibEntry}.
     */
    public void synchronizeLocalDatabase() {
        if (!checkCurrentConnection()) {
            return;
        }

        dbmsProcessor.normalizeEntryTable(); // remove unused columns

        List<BibEntry> localEntries = bibDatabase.getEntries();
        List<BibEntry> remoteEntries = dbmsProcessor.getRemoteEntries();

        for (int i = 0; i < localEntries.size(); i++) {
            BibEntry localEntry = localEntries.get(i);
            boolean match = false;
            for (int j = 0; j < remoteEntries.size(); j++) {
                if (localEntry.getRemoteId() == remoteEntries.get(j).getRemoteId()) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                bibDatabase.removeEntry(localEntry, EntryEventSource.REMOTE); // Should not reach the listeners above.
                i--; // due to index shift on localEntries
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
                        localEntry.setField(field, remoteEntry.getFieldOptional(field), EntryEventSource.REMOTE); // Should not reach the listeners above.
                    }
                }
            }
            if (!match) {
                bibDatabase.insertEntry(remoteEntry, EntryEventSource.REMOTE); // Should not reach the listeners above.
            }
        }
    }

    /**
     * Synchronizes all meta data locally.
     */
    public void synchronizeLocalMetaData() {
        if (!checkCurrentConnection()) {
            return;
        }

        try {
            metaData.setData(dbmsProcessor.getRemoteMetaData());
        } catch (ParseException e) {
            LOGGER.error("Parse error", e);
        }
    }

    /**
     * Synchronizes all meta data remotely.
     */
    public void synchronizeRemoteMetaData(MetaData data) {
        if (!checkCurrentConnection()) {
            return;
        }

        dbmsProcessor.setRemoteMetaData(data.getAsStringMap());
    }

    /**
     * Applies the {@link MetaData} on all local and remote BibEntries.
     */
    public void applyMetaData() {
        if (!checkCurrentConnection()) {
            return;
        }

        for (BibEntry entry : bibDatabase.getEntries()) {
            BibDatabaseWriter.applySaveActions(entry, metaData);
            dbmsProcessor.updateEntry(entry);
        }
    }

    /**
     * Synchronizes the local BibEntries and applies the fetched MetaData on them.
     */
    public void pullChanges() {
        if (!checkCurrentConnection()) {
            return;
        }

        synchronizeLocalDatabase();
        synchronizeLocalMetaData();
        applyMetaData();
    }

    /**
     *  Checks whether the current SQL connection is valid.
     *  In case that the connection is not valid a new {@link RemoteConnectionLostEvent} is going to be sent.
     *
     *  @return <code>true</code> if the connection is valid, else <code>false</code>.
     */
    public boolean checkCurrentConnection() {
        try {
            boolean isValid = currentConnection.isValid(0);
            if (!isValid) {
                eventBus.post(new RemoteConnectionLostEvent(bibDatabaseContext));
            }
            return isValid;

        } catch (SQLException e) {
            LOGGER.error("SQL Error:", e);
            return false;
        }
    }

    /**
     * Checks whether the {@link EntryEventSource} of an {@link EntryEvent} is crucial for this class.
     * @param event An {@link EntryEvent}
     * @return <code>true</code> if the event is able to trigger operations in {@link DBMSSynchronizer}, else <code>false</code>
     */
    public boolean isInEventLocation(EntryEvent event) {
        EntryEventSource eventLocation = event.getEntryEventLocation();
        return (eventLocation == EntryEventSource.LOCAL);
    }

    public void openRemoteDatabase(Connection connection, DBMSType type, String name) {
        this.dbmsType = type;
        this.dbName = name;
        this.dbmsProcessor = new DBMSProcessor(new DBMSHelper(connection), type);
        this.currentConnection = connection;
        initializeDatabases();
    }

    public void openRemoteDatabase(DBMSType type, String host, int port, String database, String user,
            String password) throws ClassNotFoundException, SQLException {
        openRemoteDatabase(DBMSConnector.getNewConnection(type, host, port, database, user, password), type, database);
    }

    public String getDBName() {
        return dbName;
    }

    public DBMSType getDBType() {
        return this.dbmsType;
    }

    public DBMSProcessor getDBProcessor() {
        return dbmsProcessor;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public void registerListener(Object listener) {
        eventBus.register(listener);
    }
}
