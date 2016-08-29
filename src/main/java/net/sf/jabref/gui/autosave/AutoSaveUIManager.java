package net.sf.jabref.gui.autosave;

import net.sf.jabref.autosave.event.AutosaveEvent;
import net.sf.jabref.gui.BasePanel;
import net.sf.jabref.gui.exporter.SaveDatabaseAction;

import com.google.common.eventbus.Subscribe;

public class AutoSaveUIManager {

    private final BasePanel panel;


    public AutoSaveUIManager(BasePanel panel) {
        this.panel = panel;
    }

    @Subscribe
    public void listen(@SuppressWarnings("unused") AutosaveEvent event) {
        try {
            new SaveDatabaseAction(panel).runCommand();
        } catch (Throwable e) {
            System.out.println("Problem occured while saving.");
        }
    }
}
