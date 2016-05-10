package net.sf.jabref.event;

import net.sf.jabref.event.location.EntryEventLocation;
import net.sf.jabref.model.entry.BibEntry;

/**
 * <code>EntryAddedEvent</code> is fired when a new <code>BibEntry</code> was added
 * to the database.
 */

public class EntryAddedEvent extends EntryEvent {

    /**
     * @param bibEntry <code>BibEntry</code> object which has been added.
     */
    public EntryAddedEvent(BibEntry bibEntry) {
        super(bibEntry);
    }

    /**
     * @param bibEntry <code>BibEntry</code> object which has been added.
     * @param location Location affected by this event
     */
    public EntryAddedEvent(BibEntry bibEntry, EntryEventLocation location) {
        super(bibEntry, location);
    }

}
