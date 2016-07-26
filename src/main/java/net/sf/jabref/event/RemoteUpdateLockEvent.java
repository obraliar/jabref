package net.sf.jabref.event;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.model.entry.BibEntry;

/**
 * A new {@link RemoteUpdateLockEvent} is fired, when the user tries to push changes of an obsolete
 * {@link BibEntry} to the server.
 */
public class RemoteUpdateLockEvent {

    private final BibDatabaseContext bibDatabaseContext;

    /**
     * @param bibDatabaseContext Affected {@link BibDatabaseContext}
     */
    public RemoteUpdateLockEvent(BibDatabaseContext bibDatabaseContext) {
        this.bibDatabaseContext = bibDatabaseContext;
    }

    public BibDatabaseContext getBibDatabaseContext() {
        return this.bibDatabaseContext;
    }
}
