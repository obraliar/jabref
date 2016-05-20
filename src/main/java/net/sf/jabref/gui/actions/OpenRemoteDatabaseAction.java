package net.sf.jabref.gui.actions;

import java.awt.event.ActionEvent;

import javax.swing.Action;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.Defaults;
import net.sf.jabref.Globals;
import net.sf.jabref.gui.JabRefFrame;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabaseMode;
import net.sf.jabref.model.database.DatabaseLocation;
import net.sf.jabref.remote.DBConnector;
import net.sf.jabref.remote.DBSynchronizer;
import net.sf.jabref.remote.DBType;

/**
 * The action concerned with opening a new database.
 */
public class OpenRemoteDatabaseAction extends MnemonicAwareAction {

    private final JabRefFrame jabRefFrame;


    public OpenRemoteDatabaseAction(JabRefFrame jabRefFrame) {
        super();
        this.jabRefFrame = jabRefFrame;
        putValue(Action.NAME, Localization.menuTitle("Open remote database"));
        putValue(Action.SHORT_DESCRIPTION, Localization.lang("Open remote database"));
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        // Create a new, empty, database.
        BibDatabaseContext bibDatabaseContext = new BibDatabaseContext(DatabaseLocation.REMOTE,
                new Defaults(BibDatabaseMode.BIBTEX));

        /** TODO:
         *  + GUI
         *  |
         *  |_ get host, database type (see DatabaseType.java), database name, login data, mode (see BibDatabaseMode.java)
         *  |_ use DBConnector for new connection (see below)
         *  |_ check connection (also with DBConnector.isNull(...))
         *  |_ output at frame
         */

        DBType dbType = DBType.POSTGRESQL;

        DBSynchronizer dbSynchronizer = bibDatabaseContext.getDBSynchronizer();
        dbSynchronizer.setUp(DBConnector.getNewConnection(dbType, "localhost", "xe", "admir", "q1w2e3r4"), dbType);
        dbSynchronizer.initializeLocalDatabase(bibDatabaseContext.getDatabase());

        //TODO bibDatabaseContext.setMode(mode);
        jabRefFrame.addTab(bibDatabaseContext, Globals.prefs.getDefaultEncoding(), true);

        //TODO jabRefFrame.output(Localization.lang("New %0 database created.", mode.getFormattedName()));
    }
}
