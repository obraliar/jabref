package net.sf.jabref.remote;

import net.sf.jabref.logic.l10n.Localization;

public class RemoteEntryNotPresentException extends Exception {

    public RemoteEntryNotPresentException() {
        super(Localization.lang("Required BibEntry is not present on remote database."));
    }
}
