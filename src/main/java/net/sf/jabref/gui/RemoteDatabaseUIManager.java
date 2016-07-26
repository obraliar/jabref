package net.sf.jabref.gui;

import javax.swing.JOptionPane;

import net.sf.jabref.event.RemoteConnectionLostEvent;
import net.sf.jabref.event.RemoteEntryNotPresentEvent;
import net.sf.jabref.event.RemoteUpdateLockEvent;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.DatabaseLocation;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.remote.DBMSSynchronizer;

import com.google.common.eventbus.Subscribe;

public class RemoteDatabaseUIManager {

    private final JabRefFrame jabRefFrame;
    private final DBMSSynchronizer dbmsSynchronizer;

    public RemoteDatabaseUIManager(JabRefFrame jabRefFrame) {
        this.jabRefFrame = jabRefFrame;
        this.dbmsSynchronizer = jabRefFrame.getCurrentBasePanel().getBibDatabaseContext().getDBSynchronizer();
    }

    @Subscribe
    public void listen(RemoteConnectionLostEvent connectionLostEvent) { // TODO get current Context

        jabRefFrame.output(Localization.lang("Connection lost."));

        String[] options = {Localization.lang("Reconnect"), Localization.lang("Work offline"),
                Localization.lang("Close database")};

        int answer = JOptionPane.showOptionDialog(jabRefFrame,
                Localization.lang("The connection to the server has been determinated.") + "\n\n",
                Localization.lang("Connection lost"), JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE,
                null, options, options[0]);

        if (answer == 0) {
            jabRefFrame.closeCurrentTab();
            OpenRemoteDatabaseDialog openRemoteDBDialog = new OpenRemoteDatabaseDialog(jabRefFrame);
            openRemoteDBDialog.setVisible(true);
        } else if (answer == 1) {
            connectionLostEvent.getBibDatabaseContext().updateDatabaseLocation(DatabaseLocation.LOCAL);
            jabRefFrame.refreshTitleAndTabs();
            jabRefFrame.updateEnabledState();
            jabRefFrame.output(Localization.lang("Working offline."));
        } else {
            jabRefFrame.closeCurrentTab();
        }
    }

    @Subscribe
    public void listen(RemoteUpdateLockEvent remoteUpdateLockEvent) { // TODO get current Context

        jabRefFrame.output(Localization.lang("Update refused."));

        String[] options = {Localization.lang("Pull remote changes"), Localization.lang("Leave unsynchrnoized")};

        int answer = JOptionPane.showOptionDialog(jabRefFrame,
                Localization.lang("You are not working on the newest version of BibEntry. "
                        + "Please pull remote changes to resolve this problem.") + "\n\n",
                Localization.lang("Update refused"), JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.INFORMATION_MESSAGE,
                null, options,
                options[0]);

        if (answer == 0) {
            remoteUpdateLockEvent.getBibDatabaseContext().getDBSynchronizer().pullChanges();
        }
    }

    @Subscribe
    public void listen(RemoteEntryNotPresentEvent remoteEntryNotPresentEvent) {
        BibEntry bibEntry = remoteEntryNotPresentEvent.getBibEntry();

        String[] options = {Localization.lang("Keep"), Localization.lang("Close")};

        int answer = JOptionPane.showOptionDialog(jabRefFrame,
                Localization.lang("The BibEntry you currently work on has been deleted on the remote side. "
                        + "Hit \"Keep\" to recover the entry.") + "\n\n",
                Localization.lang("Update refused"), JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.INFORMATION_MESSAGE,
                null, options, options[0]);

        if (answer == 0) {
            dbmsSynchronizer.getDBProcessor().insertEntry(bibEntry);
        } else if (answer == 1) {
            jabRefFrame.getCurrentBasePanel().hideBottomComponent();
        }
        dbmsSynchronizer.pullChanges();
    }
}
