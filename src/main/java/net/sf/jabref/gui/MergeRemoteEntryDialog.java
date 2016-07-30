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

import net.sf.jabref.gui.mergeentries.MergeEntries;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabaseMode;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.shared.DBMSSynchronizer;

public class MergeRemoteEntryDialog {

    private final JabRefFrame jabRefFrame;
    private final DBMSSynchronizer dbmsSynchronizer;
    private final BibEntry localBibEntry;
    private final BibEntry remoteBibEntry;
    private final JDialog mergeDialog;
    private final MergeEntries mergeEntries;


    public MergeRemoteEntryDialog(JabRefFrame jabRefFrame, DBMSSynchronizer dbmsSynchronizer, BibEntry localBibEntry,
            BibEntry remoteBibEntry, BibDatabaseMode bibDatabaseMode) {
        this.jabRefFrame = jabRefFrame;
        this.dbmsSynchronizer = dbmsSynchronizer;
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
        mergeDialog.add(mergeEntries.getMergeEntryPanel(), BorderLayout.CENTER);

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