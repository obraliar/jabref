/*  Copyright (C) 2003-2016 JabRef contributors.
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
package net.sf.jabref.gui;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.sql.SQLException;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.KeyStroke;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.Defaults;
import net.sf.jabref.Globals;
import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.model.database.BibDatabaseMode;
import net.sf.jabref.model.database.DatabaseLocation;
import net.sf.jabref.remote.DBConnector;
import net.sf.jabref.remote.DBSynchronizer;
import net.sf.jabref.remote.DBType;

import com.jgoodies.forms.builder.ButtonBarBuilder;

public class OpenRemoteDatabaseDialog extends JDialog {

    private final JabRefFrame frame;

    private final GridBagLayout gridBagLayout = new GridBagLayout();
    private final GridBagConstraints gridBagConstraints = new GridBagConstraints();
    private final JPanel connectionPanel = new JPanel();
    private final JPanel modePanel = new JPanel();
    private final JPanel buttonPanel = new JPanel();

    private final JLabel databaseTypeLabel = new JLabel(Localization.lang("Database type") + ":");
    private final JLabel hostPortLabel = new JLabel(Localization.lang("Host") + "/" + Localization.lang("Port") + ":");
    private final JLabel databaseLabel = new JLabel(Localization.lang("Database") + ":");
    private final JLabel userLabel = new JLabel(Localization.lang("User") + ":");
    private final JLabel passwordLabel = new JLabel(Localization.lang("Password") + ":");

    private final JTextField hostField = new JTextField(12);
    private final JTextField portField = new JTextField(4);
    private final JTextField userField = new JTextField(14);
    private final JTextField databaseField = new JTextField(14);

    private final JPasswordField passwordField = new JPasswordField(14);
    private final JComboBox<DBType> dbTypeDropDown = new JComboBox<>(new DBType[] {DBType.MYSQL, DBType.ORACLE, DBType.POSTGRESQL});

    private final JButton connectButton = new JButton(Localization.lang("Connect"));
    private final JButton cancelButton = new JButton(Localization.lang("Cancel"));

    private final ButtonGroup radioGroup = new ButtonGroup();
    private final JRadioButton radioBibTeX = new JRadioButton(BibDatabaseMode.BIBTEX.getFormattedName());
    private final JRadioButton radioBibLaTeX = new JRadioButton(BibDatabaseMode.BIBLATEX.getFormattedName());

    private static final String REMOTE_DATABASE_TYPE = "remoteDatabaseType";
    private static final String REMOTE_HOST = "remoteHost";
    private static final String REMOTE_PORT = "remotePort";
    private static final String REMOTE_DATABASE = "remoteDatabase";
    private static final String REMOTE_USER = "remoteUser";
    private static final String REMOTE_MODE = "remoteMode";


    /**
     * @param frame the JabRef Frame
     */
    public OpenRemoteDatabaseDialog(JabRefFrame frame) {
        super(frame, Localization.lang("Open remote database"));
        this.frame = frame;
        initLayout();
        applyGlobalPrefs();
        setupActions();
        pack();
    }

    /**
     * Defines and sets the different actions up.
     */
    private void setupActions() {

        Action openAction = new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    checkFields();
                    int port = Integer.parseInt(portField.getText());
                    BibDatabaseMode selectedMode = getSelectedBibDatabaseMode();
                    DBType selectedType = (DBType) dbTypeDropDown.getSelectedItem();

                    BibDatabaseContext bibDatabaseContext = new BibDatabaseContext(DatabaseLocation.REMOTE,
                            new Defaults(selectedMode));
                    DBSynchronizer dbSynchronizer = bibDatabaseContext.getDBSynchronizer();

                    //JPasswordField.getPassword() does not return a String, but a char array.
                    dbSynchronizer.setUp(DBConnector.getNewConnection(selectedType, hostField.getText(), port,
                                    databaseField.getText(), userField.getText(), new String(passwordField.getPassword())),
                                    selectedType, databaseField.getText());
                    dbSynchronizer.initializeDatabases();
                    frame.addTab(bibDatabaseContext, true);

                    setGlobalPrefs();

                    frame.output(Localization.lang("Remote_connection_to_%0_server_stablished.", selectedType.toString()));
                    dispose();
                } catch (ClassNotFoundException exception) {
                    JOptionPane.showMessageDialog(OpenRemoteDatabaseDialog.this, exception.getMessage(), Localization.lang("Driver error"),
                            JOptionPane.ERROR_MESSAGE);
                } catch (SQLException exception) {
                    JOptionPane.showMessageDialog(OpenRemoteDatabaseDialog.this, exception.getMessage(),
                            Localization.lang("Connection error"), JOptionPane.ERROR_MESSAGE);
                } catch (Exception exception) {
                    JOptionPane.showMessageDialog(OpenRemoteDatabaseDialog.this, exception.getMessage(),
                            Localization.lang("Warning"), JOptionPane.WARNING_MESSAGE);
                }
            }
        };
        connectButton.addActionListener(openAction);

        Action cancelAction = new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
            }
        };
        cancelButton.addActionListener(cancelAction);

        /**
         * Set up a listener which updates the default port number once the selection in dbTypeDropDown has changed.
         */
        Action dbTypeDropDownAction = new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                portField.setText(
                        Integer.toString(DBConnector.getDefaultPort((DBType) dbTypeDropDown.getSelectedItem())));
            }
        };
        dbTypeDropDown.addActionListener(dbTypeDropDownAction);

        // Add enter button action listener
        connectButton.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0),
                "Enter_pressed");
        connectButton.getActionMap().put("Enter_pressed", openAction);

    }

    /**
     * Fetches possibly saved data and configures the control elements respectively.
     */
    private void applyGlobalPrefs() {

        String remoteDatabaseType = Globals.prefs.get(REMOTE_DATABASE_TYPE);
        if (remoteDatabaseType != null) {
            if (remoteDatabaseType.equals(DBType.ORACLE.toString())) {
                dbTypeDropDown.setSelectedItem(DBType.ORACLE);
            } else if (remoteDatabaseType.equals(DBType.POSTGRESQL.toString())) {
                dbTypeDropDown.setSelectedItem(DBType.POSTGRESQL);
            }
        }

        hostField.setText(Globals.prefs.get(REMOTE_HOST));

        String port = Globals.prefs.get(REMOTE_PORT);
        if (port == null) {
            portField
                    .setText(Integer.toString(DBConnector.getDefaultPort((DBType) dbTypeDropDown.getSelectedItem())));
        } else {
            portField.setText(port);
        }

        databaseField.setText(Globals.prefs.get(REMOTE_DATABASE));
        userField.setText(Globals.prefs.get(REMOTE_USER));

        String mode = Globals.prefs.get(REMOTE_MODE);
        if (mode != null) {
            if (Globals.prefs.get(REMOTE_MODE).equals(BibDatabaseMode.BIBLATEX.getFormattedName())) {
                radioBibLaTeX.setSelected(true);
            } else {
                radioBibTeX.setSelected(true);
            }
        }
    }

    /**
     * Set up the layout and position the control units in their right place.
     */
    private void initLayout() {

        setResizable(false);

        Insets defautInsets = new Insets(4, 15, 4, 4);
        radioBibTeX.setSelected(true);
        radioGroup.add(radioBibTeX);
        radioGroup.add(radioBibLaTeX);

        connectionPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), Localization.lang("Connection")));
        connectionPanel.setLayout(gridBagLayout);
        modePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), Localization.lang("Mode")));
        modePanel.setLayout(gridBagLayout);

        gridBagConstraints.insets = defautInsets;
        gridBagConstraints.fill = GridBagConstraints.BOTH;
        gridBagLayout.setConstraints(connectionPanel, gridBagConstraints);

        //1. column
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        connectionPanel.add(databaseTypeLabel, gridBagConstraints);

        gridBagConstraints.gridy = 1;
        connectionPanel.add(hostPortLabel, gridBagConstraints);

        gridBagConstraints.gridy = 2;
        connectionPanel.add(databaseLabel, gridBagConstraints);

        gridBagConstraints.gridy = 3;
        connectionPanel.add(userLabel, gridBagConstraints);

        gridBagConstraints.gridy = 4;
        connectionPanel.add(passwordLabel, gridBagConstraints);

        // 2. column
        gridBagConstraints.gridwidth = 2;

        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 0;
        connectionPanel.add(dbTypeDropDown, gridBagConstraints);

        gridBagConstraints.gridy = 1;
        gridBagConstraints.gridwidth = 1; // the hostField is smaller than the others.
        gridBagConstraints.insets = new Insets(4, 15, 4, 0);
        connectionPanel.add(hostField, gridBagConstraints);

        gridBagConstraints.gridy = 2;
        gridBagConstraints.gridwidth = 2;
        gridBagConstraints.insets = defautInsets;
        connectionPanel.add(databaseField, gridBagConstraints);

        gridBagConstraints.gridy = 3;
        connectionPanel.add(userField, gridBagConstraints);

        gridBagConstraints.gridy = 4;
        connectionPanel.add(passwordField, gridBagConstraints);

        // 3. column
        gridBagConstraints.gridx = 2;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.gridwidth = 1;
        gridBagConstraints.insets = new Insets(4, 0, 4, 4);
        connectionPanel.add(portField, gridBagConstraints);

        gridBagConstraints.insets = new Insets(4, 4, 4, 4);
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        modePanel.add(radioBibTeX, gridBagConstraints);
        gridBagConstraints.gridx = 1;
        modePanel.add(radioBibLaTeX, gridBagConstraints);

        ButtonBarBuilder bsb = new ButtonBarBuilder(buttonPanel);
        bsb.addGlue();
        bsb.addButton(connectButton);
        bsb.addRelatedGap();
        bsb.addButton(cancelButton);

        getContentPane().setLayout(gridBagLayout);

        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        gridBagLayout.setConstraints(connectionPanel, gridBagConstraints);
        getContentPane().add(connectionPanel);
        gridBagConstraints.gridy = 1;
        gridBagLayout.setConstraints(modePanel, gridBagConstraints);
        getContentPane().add(modePanel);
        gridBagConstraints.gridy = 2;
        gridBagConstraints.insets = new Insets(12, 4, 12, 4);
        gridBagLayout.setConstraints(buttonPanel, gridBagConstraints);
        getContentPane().add(buttonPanel);

        setModal(true); // Owner window should be disabled while this dialog is opened.
    }

    /**
     * Saves the data from this dialog persistently to facilitate the usage.
     */
    public void setGlobalPrefs() {
        Globals.prefs.put(REMOTE_DATABASE_TYPE, ((DBType) dbTypeDropDown.getSelectedItem()).toString());
        Globals.prefs.put(REMOTE_HOST, hostField.getText());
        Globals.prefs.put(REMOTE_PORT, portField.getText());
        Globals.prefs.put(REMOTE_DATABASE, databaseField.getText());
        Globals.prefs.put(REMOTE_USER, userField.getText());
        Globals.prefs.put(REMOTE_MODE, getSelectedBibDatabaseMode().getFormattedName());
    }

    /**
     * Retrieves the chosen bib editing mode in this dialog.
     */
    private BibDatabaseMode getSelectedBibDatabaseMode() {
        BibDatabaseMode selectedMode = BibDatabaseMode.BIBTEX;
        if (radioBibLaTeX.isSelected()) {
            selectedMode = BibDatabaseMode.BIBLATEX;
        }
        return selectedMode;
    }

    private boolean isEmptyField(JTextField field) {
        return field.getText().trim().length() == 0;
    }

    /**
     * Checks every required text field for emptiness.
     */
    private void checkFields() throws Exception {
        if (isEmptyField(hostField)) {
            hostField.requestFocus();
            throw new Exception(Localization.lang("Required_field_\"%0\"_is_empty.", Localization.lang("Host")));
        }
        if (isEmptyField(portField)) {
            portField.requestFocus();
            throw new Exception(Localization.lang("Required_field_\"%0\"_is_empty.", Localization.lang("Port")));
        }
        if (isEmptyField(databaseField)) {
            databaseField.requestFocus();
            throw new Exception(Localization.lang("Required_field_\"%0\"_is_empty.", Localization.lang("Database")));
        }
        if (isEmptyField(userField)) {
            userField.requestFocus();
            throw new Exception(Localization.lang("Required_field_\"%0\"_is_empty.", Localization.lang("User")));
        }
        if (isEmptyField(passwordField)) {
            passwordField.requestFocus();
            throw new Exception(Localization.lang("Required_field_\"%0\"_is_empty.", Localization.lang("Password")));
        }

    }

}