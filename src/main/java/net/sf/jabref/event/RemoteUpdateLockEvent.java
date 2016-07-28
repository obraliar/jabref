package net.sf.jabref.event;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.model.entry.BibEntry;

/**
 * A new {@link RemoteUpdateLockEvent} is fired, when the user tries to push changes of an obsolete
 * {@link BibEntry} to the server.
 */
public class RemoteUpdateLockEvent {

    private final BibDatabaseContext bibDatabaseContext;
    private final BibEntry localBibEntry;
    private final BibEntry remoteBibEntry;

    /**
     * @param bibDatabaseContext Affected {@link BibDatabaseContext}
     * @param bibEntry Affected {@link BibEntry}
     */
    public RemoteUpdateLockEvent(BibDatabaseContext bibDatabaseContext, BibEntry localBibEntry, BibEntry remoteBibEntry) {
        this.bibDatabaseContext = bibDatabaseContext;
        this.localBibEntry = localBibEntry;
        this.remoteBibEntry = remoteBibEntry;
    }

    public BibDatabaseContext getBibDatabaseContext() {
        return this.bibDatabaseContext;
    }

    public BibEntry getLocalBibEntry() {
        return localBibEntry;
    }

    public BibEntry getRemoteBibEntry() {
        return remoteBibEntry;
    }
}
