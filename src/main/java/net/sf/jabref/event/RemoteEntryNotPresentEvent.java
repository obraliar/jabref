package net.sf.jabref.event;

import net.sf.jabref.model.entry.BibEntry;

/**
 * A new {@link RemoteEntryNotPresentEvent} is fired, when the user tries to push changes of an obsolete
 * {@link BibEntry} to the server.
 */
public class RemoteEntryNotPresentEvent {

    private final BibEntry bibEntry;

    /**
     * @param bibEntry Affected {@link BibEntry}
     */
    public RemoteEntryNotPresentEvent(BibEntry bibEntry) {
        this.bibEntry = bibEntry;
    }

    public BibEntry getBibEntry() {
        return this.bibEntry;
    }
}
