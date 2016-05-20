package net.sf.jabref.remote;

import java.sql.Connection;
import net.sf.jabref.event.EntryAddedEvent;
import net.sf.jabref.event.EntryRemovedEvent;
import net.sf.jabref.event.FieldChangedEvent;
import net.sf.jabref.model.database.BibDatabase;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.Subscribe;

public class DBSynchronizer {

    private Connection connection;
    private final DBHelper dbHelper = new DBHelper();
    private DBType dbType;


    @Subscribe
    public void listen(EntryAddedEvent event) {
        dbHelper.insertEntry(event.getBibEntry());
    }

    @Subscribe
    public void listen(FieldChangedEvent event) {
        dbHelper.updateEntry(event.getBibEntry(), event.getFieldName(), event.getNewValue());
    }

    @Subscribe
    public void listen(EntryRemovedEvent event) {
        dbHelper.removeEntry(event.getBibEntry());
    }

    // TODO synchronizer: ignore fields jabref_database, type ...
    public void initializeLocalDatabase(BibDatabase bibDatabase) {

        if (!dbHelper.checkIntegrity()) {
            System.out.println("checkIntegrity: NOT OK. Fixing...");
            dbHelper.setUpRemoteDatabase(this.dbType);
        }

        for (BibEntry bibEntry : dbHelper.getRemoteEntries()) {
            bibDatabase.insertEntry(bibEntry);
        }

    }

    public void setConnection(Connection connection) {
        this.connection = connection;
        this.dbHelper.setConnection(connection);
    }

    // TODO getter: database name (probably not the right place)
    public String getRemoteDatabaseName() {
        return "test123";
    }

    public void setDBType(DBType dbType) {
        this.dbType = dbType;
        dbHelper.setDBType(dbType);
    }

    public DBType getDBType() {
        return this.dbType;
    }

}
