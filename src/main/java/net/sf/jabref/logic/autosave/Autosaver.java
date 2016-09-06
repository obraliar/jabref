package net.sf.jabref.logic.autosave;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import net.sf.jabref.BibDatabaseContext;
import net.sf.jabref.event.BibDatabaseContextChangedEvent;

import com.google.common.eventbus.Subscribe;

public class Autosaver {

    private final File backupFile;
    private final BibDatabaseContext bibDatabaseContext;


    public Autosaver(File originalFile, BibDatabaseContext bibDatabaseContext) {
        this.backupFile = new File(getUniqueBackupFilePath(originalFile.getAbsolutePath()));
        this.bibDatabaseContext = bibDatabaseContext;
    }

    /*public BackupManager(BibDatabaseContext bibDatabaseContext) throws IOException {
        this.backupFile = File.createTempFile("asas", "tmp.bib");

    }*/

    @Subscribe
    public synchronized void storeBackup(@SuppressWarnings("unused") BibDatabaseContextChangedEvent event) {
        new Thread(new AutosaveWorker(backupFile, bibDatabaseContext)).start();
    }

    public void restoreBackup() {
        //TODO
    }

    private String getUniqueBackupFilePath(String originalFilePath) {
        int uniqueBackupId = (int) (System.currentTimeMillis() / 1000L);
        Path backupPath;

        do {
            backupPath = Paths.get(originalFilePath + "$" + getAlnumString(uniqueBackupId) + "$");
            uniqueBackupId++;
        } while (Files.exists(backupPath) || Files.isDirectory(backupPath));

        return backupPath.toString();
    }

    //    private String getUniqueTempBackupFilePath() {
    //
    //    }

    private String getAlnumString(int intToConvert) {
        return Integer.toString(intToConvert, 36); // [0-9a-z]
    }

}
