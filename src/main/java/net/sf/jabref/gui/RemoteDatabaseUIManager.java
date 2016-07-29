package net.sf.jabref.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import net.sf.jabref.event.RemoteConnectionLostEvent;
import net.sf.jabref.event.RemoteEntryNotPresentEvent;
import net.sf.jabref.event.RemoteUpdateLockEvent;
import net.sf.jabref.gui.mergeentries.MergeEntries;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabaseMode;
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

        new MergeRemoteEntryDialog(remoteUpdateLockEvent.getLocalBibEntry(), remoteUpdateLockEvent.getRemoteBibEntry(),
                    remoteUpdateLockEvent.getBibDatabaseContext().getMode()).showMergeDialog();
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


    private class MergeRemoteEntryDialog {

        private final BibEntry localBibEntry;
        private final BibEntry remoteBibEntry;
        private final JDialog mergeDialog;
        private final MergeEntries mergeEntries;


        public MergeRemoteEntryDialog(BibEntry localBibEntry, BibEntry remoteBibEntry, BibDatabaseMode bibDatabaseMode) {
            this.localBibEntry = localBibEntry;
            this.remoteBibEntry = remoteBibEntry;
            this.mergeDialog = new JDialog(jabRefFrame, Localization.lang("Update refused"), true);
            this.mergeEntries = new MergeEntries(localBibEntry, remoteBibEntry, "Local BibEntry", "Remote BibEntry", bibDatabaseMode);
        }

        public void showMergeDialog() {

            mergeDialog.setMinimumSize(new Dimension(600, 600));

            StringBuilder message = new StringBuilder();
            message.append("<html>");
            message.append("<b>Update could not be performed due to existing change conflicts.</b>");
            message.append("<br/><br/>");
            message.append("You are not working on the newest version of BibEntry.<br/><br/>"
                    + "Your version: " + localBibEntry.getVersion() + "<br/>"
                    + "Remote version: " + remoteBibEntry.getVersion() + "<br/><br/>"
                    + "Please merge the remote version with yours and press \"Merge entries\" to resolve this problem.<br/>");

            JLabel mergeInnformation = new JLabel(message.toString());
            mergeInnformation.setBorder(new EmptyBorder(9, 9, 9, 9));

            mergeDialog.add(mergeInnformation, BorderLayout.NORTH);
            mergeDialog.add(mergeEntries.getMergeEntryPanel(), BorderLayout.CENTER); ///////////////// TODO Update version and remoteId

            JButton mergeButton = new JButton(Localization.lang("Merge entries"));
            mergeButton.addActionListener(e -> mergeEntries());

            JButton cancelButton = new JButton(Localization.lang("Cancel"));
            cancelButton.addActionListener(e -> showConfirmationDialog());

            JPanel buttonPanel = new JPanel();
            buttonPanel.add(mergeButton, BorderLayout.WEST);
            buttonPanel.add(cancelButton, BorderLayout.EAST);

            mergeDialog.add(buttonPanel, BorderLayout.SOUTH);
            mergeDialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
            mergeDialog.addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosing(WindowEvent e) {
                    showConfirmationDialog();
                }
            });

            mergeDialog.setLocationRelativeTo(jabRefFrame);
            mergeDialog.pack();
            mergeDialog.setVisible(true);
        }

        private void showConfirmationDialog() {
            String[] options = {Localization.lang("Yes"), Localization.lang("No")};

            int answer = JOptionPane.showOptionDialog(mergeDialog,
                    Localization.lang("Canceling this operation will leave your changes unsynchronized. Cancel anyway?"),
                    Localization.lang("Warning"), JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE,
                    null, options, options[1]);
            if (answer == 0) {
                mergeDialog.dispose();
            }
        }

        private void mergeEntries() {
            BibEntry mergedBibEntry = mergeEntries.getMergeEntry();
            mergedBibEntry.setRemoteId(remoteBibEntry.getRemoteId());
            mergedBibEntry.setVersion(remoteBibEntry.getVersion());

            mergeDialog.dispose(); // dispose before synchronizing to avoid multiple merge windows in case of new conflict.

            dbmsSynchronizer.synchrnizeRemoteEntry(mergedBibEntry);
            dbmsSynchronizer.synchronizeLocalDatabase();
        }
    }
}