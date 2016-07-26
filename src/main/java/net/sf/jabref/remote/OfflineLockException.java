package net.sf.jabref.remote;

import net.sf.jabref.logic.l10n.Localization;

public class OfflineLockException extends Exception {

    public OfflineLockException() {
        super(Localization.lang("Local BibEntry data is not up-to-date"));
    }
}
