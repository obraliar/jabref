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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.MetaData;
import net.sf.jabref.event.MetaDataChangedEvent;
import net.sf.jabref.event.source.EntryEventSource;
import net.sf.jabref.importer.fileformat.ParseException;
import net.sf.jabref.logic.exporter.BibDatabaseWriter;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.model.event.EntryAddedEvent;
import net.sf.jabref.model.event.EntryEvent;
import net.sf.jabref.model.event.EntryRemovedEvent;
import net.sf.jabref.model.event.FieldChangedEvent;
import net.sf.jabref.shared.event.ConnectionLostEvent;
import net.sf.jabref.shared.event.SharedEntryNotPresentEvent;
import net.sf.jabref.shared.event.UpdateRefusedEvent;
import net.sf.jabref.shared.exception.OfflineLockException;
import net.sf.jabref.shared.exception.SharedEntryNotPresentException;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Synchronizes the shared or local databases with their opposite side.
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
     * Listening method. Inserts a new {@link BibEntry} into shared database.
     * @param event {@link EntryAddedEvent} object
     */
    @Subscribe
    public void listen(EntryAddedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to insert the bibEntry entry again (but it would not harm).
        if (isEventSourceAccepted(event) && checkCurrentConnection()) {
            dbmsProcessor.insertEntry(event.getBibEntry());
            synchronizeLocalMetaData();
            synchronizeLocalDatabase(); // Pull changes for the case that there were some
        }
    }

    /**
     * Listening method. Updates an existing shared {@link BibEntry}.
     * @param event {@link FieldChangedEvent} object
     */
    @Subscribe
    public void listen(FieldChangedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to update the bibEntry entry again (but it would not harm).
        if (isEventSourceAccepted(event) && checkCurrentConnection()) {
            synchronizeLocalMetaData();
            try {
                BibEntry bibEntry = event.getBibEntry();
                BibDatabaseWriter.applySaveActions(bibEntry, metaData); // perform possibly existing save actions
                // do not use newValue from the event cause save actions are only applied on the entry
                //dbmsProcessor.updateField(event.getBibEntry(), event.getFieldName());
                dbmsProcessor.updateEntry(event.getBibEntry());
                synchronizeLocalDatabase(); // Pull changes for the case that there were some
            } catch (OfflineLockException exception) {
                eventBus.post(new UpdateRefusedEvent(bibDatabaseContext, exception.getLocalBibEntry(), exception.getSharedBibEntry()));
            } catch (SharedEntryNotPresentException exception) {
                eventBus.post(new SharedEntryNotPresentEvent(exception.getNonPresentBibEntry()));
            } catch (SQLException e) {
                LOGGER.error("SQL Error: ", e);
            }
        }
    }

    /**
     * Listening method. Deletes the given {@link BibEntry} from shared database.
     * @param event {@link EntryRemovedEvent} object
     */
    @Subscribe
    public void listen(EntryRemovedEvent event) {
        // While synchronizing the local database (see synchronizeLocalDatabase() below), some EntryEvents may be posted.
        // In this case DBSynchronizer should not try to delete the bibEntry entry again (but it would not harm).
        if (isEventSourceAccepted(event) && checkCurrentConnection()) {
            dbmsProcessor.removeEntry(event.getBibEntry());
            synchronizeLocalMetaData();
            synchronizeLocalDatabase(); // Pull changes for the case that there where some
        }
    }

    /**
     * Listening method. Synchronizes the shared {@link MetaData} and applies them locally.
     * @param event
     */
    @Subscribe
    public void listen(MetaDataChangedEvent event) {
        if (checkCurrentConnection()) {
            synchronizeSharedMetaData(event.getMetaData());
            applyMetaData();
            synchronizeLocalDatabase();
        }
    }

    /**
     * Sets the table structure of shared database if needed and pulls all shared entries
     * to the new local database.
     * @param bibDatabase Local {@link BibDatabase}
     */
    public void initializeDatabases() {
        try {
            if (!dbmsProcessor.checkBaseIntegrity()) {
                LOGGER.info(Localization.lang("Integrity check failed. Fixing..."));
                dbmsProcessor.setUpSharedDatabase();
            }
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
        synchronizeLocalMetaData();
        synchronizeLocalDatabase();
    }

    /**
     * Synchronizes the local database with shared one.
     * Possible update types are removal, update or insert of a {@link BibEntry}.
     */
    public void synchronizeLocalDatabase() {
        if (!checkCurrentConnection()) {
            return;
        }

        List<BibEntry> localEntries = bibDatabase.getEntries();
        Map<Integer, Integer> idVersionMap = dbmsProcessor.getSharedIDVersionMapping();

        // remove old entries locally
        removeNotSharedEntries(localEntries, idVersionMap.keySet());

        // compare versions and update local entry if needed
        for (Map.Entry<Integer, Integer> idVersionEntry : idVersionMap.entrySet()) {
            boolean match = false;
            for (BibEntry localEntry : localEntries) {
                if (idVersionEntry.getKey() == localEntry.getSharedID()) {
                    match = true;
                    if (idVersionEntry.getValue() > localEntry.getVersion()) {
                        Optional<BibEntry> sharedEntry = dbmsProcessor.getSharedEntry(idVersionEntry.getKey());
                        if (sharedEntry.isPresent()) {
                            // update fields
                            localEntry.setType(sharedEntry.get().getType(), EntryEventSource.SHARED);
                            localEntry.setVersion(sharedEntry.get().getVersion());
                            for (String field : sharedEntry.get().getFieldNames()) {
                                localEntry.setField(field, sharedEntry.get().getFieldOptional(field), EntryEventSource.SHARED);
                            }

                            Set<String> redundantLocalEntryFields = localEntry.getFieldNames();
                            redundantLocalEntryFields.removeAll(sharedEntry.get().getFieldNames());

                            // remove not existing fields
                            for (String redundantField : redundantLocalEntryFields) {
                                localEntry.clearField(redundantField, EntryEventSource.SHARED);
                            }
                        }
                    }
                }
            }
            if (!match) {
                Optional<BibEntry> bibEntry = dbmsProcessor.getSharedEntry(idVersionEntry.getKey());
                if (bibEntry.isPresent()) {
                    bibDatabase.insertEntry(bibEntry.get(), EntryEventSource.SHARED);
                }
            }
        }
    }

    /**
     * Removes all local entries which are not present on shared database.
     *
     * @param localEntries List of {@link BibEntry} the entries should be removed from
     * @param sharedIDs Set of all IDs which are present on shared database
     */
    private void removeNotSharedEntries(List<BibEntry> localEntries, Set<Integer> sharedIDs) {
        for (int i = 0; i < localEntries.size(); i++) {
            BibEntry localEntry = localEntries.get(i);
            boolean match = false;
            for (int sharedID : sharedIDs) {
                if (localEntry.getSharedID() == sharedID) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                bibDatabase.removeEntry(localEntry, EntryEventSource.SHARED); // Should not reach the listeners above.
                i--; // due to index shift on localEntries
            }
        }
    }

    /**
     * Synchronizes the shared {@link BibEntry} with the local one.
     * Save actions are applied before updating.
     */
    public void synchrnizeSharedEntry(BibEntry bibEntry) {
        if (!checkCurrentConnection()) {
            return;
        }
        try {
            BibDatabaseWriter.applySaveActions(bibEntry, metaData);
            dbmsProcessor.updateEntry(bibEntry);
        } catch (OfflineLockException exception) {
            eventBus.post(new UpdateRefusedEvent(bibDatabaseContext, exception.getLocalBibEntry(), exception.getSharedBibEntry()));
        } catch (SharedEntryNotPresentException exception) {
            eventBus.post(new SharedEntryNotPresentEvent(exception.getNonPresentBibEntry()));
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
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
            metaData.setData(dbmsProcessor.getSharedMetaData());
        } catch (ParseException e) {
            LOGGER.error("Parse error", e);
        }
    }

    /**
     * Synchronizes all shared meta data.
     */
    public void synchronizeSharedMetaData(MetaData data) {
        if (!checkCurrentConnection()) {
            return;
        }
        try {
            dbmsProcessor.setSharedMetaData(data.getAsStringMap());
        } catch (SQLException e) {
            LOGGER.error("SQL Error: ", e);
        }
    }

    /**
     * Applies the {@link MetaData} on all local and shared BibEntries.
     */
    public void applyMetaData() {
        if (!checkCurrentConnection()) {
            return;
        }
        for (BibEntry bibEntry : bibDatabase.getEntries()) {
            synchrnizeSharedEntry(bibEntry);
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
    }

    /**
     *  Checks whether the current SQL connection is valid.
     *  In case that the connection is not valid a new {@link ConnectionLostEvent} is going to be sent.
     *
     *  @return <code>true</code> if the connection is valid, else <code>false</code>.
     */
    public boolean checkCurrentConnection() {
        try {
            boolean isValid = currentConnection.isValid(0);
            if (!isValid) {
                eventBus.post(new ConnectionLostEvent(bibDatabaseContext));
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
    public boolean isEventSourceAccepted(EntryEvent event) {
        EntryEventSource eventSource = event.getEntryEventSource();
        return ((eventSource == EntryEventSource.LOCAL) || (eventSource == EntryEventSource.UNDO));
    }

    public void openSharedDatabase(Connection connection, DBMSType type, String name) {
        this.dbmsType = type;
        this.dbName = name;
        this.currentConnection = connection;
        this.dbmsProcessor = DBMSProcessor.getProcessorInstance(connection, type);
        initializeDatabases();
    }

    public void openSharedDatabase(DBMSConnectionProperties properties) throws ClassNotFoundException, SQLException {
        openSharedDatabase(
                DBMSConnector.getNewConnection(
                        properties.getType(),
                        properties.getHost(),
                        properties.getPort(),
                        properties.getDatabase(),
                        properties.getUser(),
                        properties.getPassword()),
                properties.getType(),
                properties.getDatabase());
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
