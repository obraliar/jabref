package net.sf.jabref.logic.autosave;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Optional;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.Globals;
import net.sf.jabref.logic.exporter.BibDatabaseWriter;
import net.sf.jabref.logic.exporter.BibtexDatabaseWriter;
import net.sf.jabref.logic.exporter.FileSaveSession;
import net.sf.jabref.logic.exporter.SaveException;
import net.sf.jabref.logic.exporter.SavePreferences;
import net.sf.jabref.logic.exporter.SaveSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AutosaveWorker implements Runnable {

    private static final Log LOGGER = LogFactory.getLog(AutosaveWorker.class);

    private final File backupFile;
    private final Charset charset;
    private final BibDatabaseContext bibDatabaseContext;


    public AutosaveWorker(File backupFile, BibDatabaseContext bibDatabaseContext) {
        this.backupFile = backupFile;
        this.bibDatabaseContext = bibDatabaseContext;

        Optional<Charset> charsetOptional = bibDatabaseContext.getMetaData().getEncoding();
        this.charset = charsetOptional.isPresent() ? charsetOptional.get() : Globals.prefs.getDefaultEncoding();
    }

    @Override
    public void run() {
        System.out.println("Create backup...");
        try {
            SavePreferences prefs = SavePreferences.loadForSaveFromPreferences(Globals.prefs).withEncoding(charset);
            BibDatabaseWriter<FileSaveSession> databaseWriter = new BibtexDatabaseWriter<>(FileSaveSession::new);
            SaveSession ss = databaseWriter.saveDatabase(bibDatabaseContext, prefs);
            ss.commit(backupFile.toPath());
        } catch (SaveException e) {
            LOGGER.error("Problem occured during automatic save.", e);
        }
    }
}
