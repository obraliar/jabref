package net.sf.jabref.shared;

import net.sf.jabref.model.entry.BibEntry;

public class OfflineLockException extends Exception {

    private final BibEntry localBibEntry;
    private final BibEntry sharedBibEntry;


    public OfflineLockException(BibEntry localBibEntry, BibEntry sharedBibEntry) {
        super("Local BibEntry data is not up-to-date.");
        this.localBibEntry = localBibEntry;
        this.sharedBibEntry = sharedBibEntry;
    }

    public BibEntry getLocalBibEntry() {
        return localBibEntry;
    }

    public BibEntry getSharedBibEntry() {
        return sharedBibEntry;
    }
}
