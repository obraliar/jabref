/*  Copyright (C) 2003-2015 JabRef contributors.
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
import java.awt.Window;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import net.sf.jabref.logic.l10n.Localization;
import net.sf.jabref.remote.DBType;

import com.jgoodies.forms.builder.ButtonBarBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OpenRemoteDatabaseDialog extends JDialog {

    private final JabRefFrame frame;

    private final GridBagLayout gridBagLayout = new GridBagLayout();
    private final GridBagConstraints gridBagConstraints = new GridBagConstraints();
    private final JPanel connectionPanel = new JPanel();
    private final JPanel modePanel = new JPanel();
    private final JPanel buttonPan = new JPanel();

    private final JLabel databaseTypeLabel = new JLabel("Database type:"); //local...
    private final JLabel hostPortLabel = new JLabel("Host/Port:"); //local...
    private final JLabel databaseLabel = new JLabel("Database:"); //local...
    private final JLabel userLabel = new JLabel("User:"); //local...
    private final JLabel passwordLabel = new JLabel("Password:"); //local...


    private final JTextField hostField = new JTextField(12);
    private final JTextField userField = new JTextField(14);
    private final JTextField databaseField = new JTextField(14);
    private final JPasswordField passwordField = new JPasswordField(14);

    private final JComboBox<DBType> dbType = new JComboBox<>(
            new DBType[] {DBType.MYSQL, DBType.ORACLE, DBType.POSTGRESQL});

    private final JTextField portSpinner = new JTextField(4);
    /*private final JSpinner portSpinner = new JSpinner(
            new SpinnerNumberModel(DBConnector.getDefaultPort(DBType.MYSQL), 0, 65535, 1));*/

    private final JButton ok = new JButton(Localization.lang("Open")); //Local...
    private final JButton open = new JButton();

    private final ButtonGroup radioGroup = new ButtonGroup();
    private final JRadioButton radioBibTeX = new JRadioButton("BibTeX");
    private final JRadioButton radioBibLaTeX = new JRadioButton("BibLaTeX");

    private static final Log LOGGER = LogFactory.getLog(OpenRemoteDatabaseDialog.class);

    /**
     * @param owner the parent Window (Dialog or Frame)
     * @param frame the JabRef Frame
     */
    public OpenRemoteDatabaseDialog(Window owner, JabRefFrame frame) {
        super(owner, Localization.lang("Open remote database"));
        this.frame = frame;
        initLayout();
        setupActions();
        pack();
        setModal(true);
    }

    private void setupActions() {

        ok.addActionListener(e -> {
            try {
                dispose();
            } catch (Exception ex) {
                LOGGER.info("Could not apply changes in \"Setup selectors\"", ex);
                JOptionPane.showMessageDialog(frame, Localization.lang("Could not apply changes."));
            }
        });

        Action cancelAction = new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
            }
        };

        cancelAction.putValue(Action.NAME, Localization.lang("Cancel"));
        open.setAction(cancelAction);
    }

    private void initLayout() {

        setResizable(false);

        Insets defautInsets = new Insets(4, 15, 4, 4);

        radioBibTeX.setSelected(true);
        radioGroup.add(radioBibTeX);
        radioGroup.add(radioBibLaTeX);

        connectionPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Connection")); //Localization.lang("Field name")));
        connectionPanel.setLayout(gridBagLayout);
        modePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Mode"));
        modePanel.setLayout(gridBagLayout);

        gridBagConstraints.insets = defautInsets;
        gridBagConstraints.fill = GridBagConstraints.BOTH;
        gridBagLayout.setConstraints(connectionPanel, gridBagConstraints);

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
        connectionPanel.add(dbType, gridBagConstraints);

        gridBagConstraints.gridy = 1;
        gridBagConstraints.gridwidth = 1;
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
        connectionPanel.add(portSpinner, gridBagConstraints);

        gridBagConstraints.insets = new Insets(4, 4, 4, 4);
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        modePanel.add(radioBibTeX, gridBagConstraints);
        gridBagConstraints.gridx = 1;
        modePanel.add(radioBibLaTeX, gridBagConstraints);

        ButtonBarBuilder bsb = new ButtonBarBuilder(buttonPan);
        bsb.addGlue();
        bsb.addButton(ok);
        bsb.addRelatedGap();
        bsb.addButton(open);
        //bsb.addButton(new HelpAction(HelpFiles.CONTENT_SELECTOR).getHelpButton());
        bsb.addGlue();

        gridBagConstraints.fill = GridBagConstraints.BOTH;
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
        gridBagLayout.setConstraints(buttonPan, gridBagConstraints);
        getContentPane().add(buttonPan);

    }

}
