package net.sf.jabref.gui.actions;

import java.awt.event.ActionEvent;

import javax.swing.Action;

import net.sf.jabref.gui.JabRefFrame;
import net.sf.jabref.gui.OpenRemoteDatabaseDialog;
import net.sf.jabref.logic.l10n.Localization;

/**
 * The action concerned with opening a remote database.
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

        /*BibDatabaseContext bibDatabaseContext = new BibDatabaseContext(DatabaseLocation.REMOTE,
                new Defaults(BibDatabaseMode.BIBTEX));

        /** TODO:
         *  + GUI
         *  |
         *  |_ get host, database type (see DatabaseType.java), database name, login data, mode (see BibDatabaseMode.java)
         *  |_ use DBConnector for new connection (see below)
         *  |_ check connection (also with DBConnector.isNull(...))
         *  |_ output at frame
         */

        /*
        DBType dbType = DBType.ORACLE;
        String host = "localhost";
        String user = "admir";
        String password = "q1w2e3r4";
        String dbName = "xe";*/

        /*DBSynchronizer dbSynchronizer = bibDatabaseContext.getDBSynchronizer();
        dbSynchronizer.setUp(DBConnector.getNewConnection(dbType, host, dbName, user, password), dbType, dbName);
        dbSynchronizer.initializeLocalDatabase(bibDatabaseContext.getDatabase());
        //TODO bibDatabaseContext.setMode(mode);
        jabRefFrame.addTab(bibDatabaseContext, Globals.prefs.getDefaultEncoding(), true);*/

        //TODO jabRefFrame.output(Localization.lang("New %0 database created.", mode.getFormattedName()));
        OpenRemoteDatabaseDialog csd = new OpenRemoteDatabaseDialog(jabRefFrame);
        csd.setLocationRelativeTo(jabRefFrame);
        csd.setVisible(true);
    }
}
