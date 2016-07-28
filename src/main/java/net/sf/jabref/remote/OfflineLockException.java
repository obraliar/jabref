package net.sf.jabref.remote;

import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.entry.BibEntry;

public class OfflineLockException extends Exception {

    private final BibEntry localBibEntry;
    private final BibEntry remoteBibEntry;


    public OfflineLockException(BibEntry localBibEntry, BibEntry remoteBibEntry) {
        super(Localization.lang("Local BibEntry data is not up-to-date."));
        this.localBibEntry = localBibEntry;
        this.remoteBibEntry = remoteBibEntry;
    }

    public BibEntry getLocalBibEntry() {
        return localBibEntry;
    }

    public BibEntry getRemoteBibEntry() {
        return remoteBibEntry;
    }
}
