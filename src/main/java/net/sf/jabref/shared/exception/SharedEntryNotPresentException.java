package net.sf.jabref.shared.exception;

import net.sf.jabref.model.entry.BibEntry;

public class SharedEntryNotPresentException extends Exception {

    private final BibEntry nonPresentbibEntry;


    public SharedEntryNotPresentException(BibEntry nonPresentbibEntry) {
        super("Required BibEntry is not present on shared database.");
        this.nonPresentbibEntry = nonPresentbibEntry;
    }

    public BibEntry getNonPresentBibEntry() {
        return this.nonPresentbibEntry;
    }
}
