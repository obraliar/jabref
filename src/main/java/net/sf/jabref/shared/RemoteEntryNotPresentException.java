package net.sf.jabref.shared;

import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.entry.BibEntry;

public class RemoteEntryNotPresentException extends Exception {

    private final BibEntry nonPresentbibEntry;


    public RemoteEntryNotPresentException(BibEntry nonPresentbibEntry) {
        super(Localization.lang("Required BibEntry is not present on remote database."));
        this.nonPresentbibEntry = nonPresentbibEntry;
    }

    public BibEntry getNonPresentBibEntry() {
        return this.nonPresentbibEntry;
    }
}
