package net.sf.jabref.shared.prefs;

import java.util.Optional;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import net.sf.jabref.JabRefMain;
import net.sf.jabref.gui.shared.OpenSharedDatabaseDialog;

/**
 * Stores and reads persistent data for {@link OpenSharedDatabaseDialog}.
 */
public class SharedDatabasePreferences {

    private static final String DEFAULT_NODE = "default";
    private static final String MAIN_NODE = "jabref-shared";

    private static final String SHARED_DATABASE_TYPE = "sharedDatabaseType";
    private static final String SHARED_DATABASE_HOST = "sharedDatabaseHost";
    private static final String SHARED_DATABASE_PORT = "sharedDatabasePort";
    private static final String SHARED_DATABASE_NAME = "sharedDatabaseName";
    private static final String SHARED_DATABASE_USER = "sharedDatabaseUser";
    private static final String SHARED_DATABASE_PASSWORD = "sharedDatabasePassword";
    private static final String SHARED_DATABASE_REMEMBER_PASSWORD = "sharedDatabaseRememberPassword";

    // This {@link Preferences} is used only for things which should not appear in real JabRefPreferences due to security reasons.
    private final Preferences internalPrefs;


    public SharedDatabasePreferences() {
        this(DEFAULT_NODE);
    }

    public SharedDatabasePreferences(String databaseID) {
        internalPrefs = Preferences.userNodeForPackage(JabRefMain.class).parent().node(MAIN_NODE).node(databaseID);
    }

    public Optional<String> getType() {
        return getOptionalValue(SHARED_DATABASE_TYPE);
    }

    public Optional<String> getHost() {
        return getOptionalValue(SHARED_DATABASE_HOST);
    }

    public Optional<String> getPort() {
        return getOptionalValue(SHARED_DATABASE_PORT);
    }

    public Optional<String> getName() {
        return getOptionalValue(SHARED_DATABASE_NAME);
    }

    public Optional<String> getUser() {
        return getOptionalValue(SHARED_DATABASE_USER);
    }

    public Optional<String> getPassword() {
        return getOptionalValue(SHARED_DATABASE_PASSWORD);
    }

    public boolean getRememberPassword() {
        return internalPrefs.getBoolean(SHARED_DATABASE_REMEMBER_PASSWORD, false);
    }

    public void setType(String type) {
        internalPrefs.put(SHARED_DATABASE_TYPE, type);
    }

    public void setHost(String host) {
        internalPrefs.put(SHARED_DATABASE_HOST, host);
    }

    public void setPort(String port) {
        internalPrefs.put(SHARED_DATABASE_PORT, port);
    }

    public void setName(String name) {
        internalPrefs.put(SHARED_DATABASE_NAME, name);
    }

    public void setUser(String user) {
        internalPrefs.put(SHARED_DATABASE_USER, user);
    }

    public void setPassword(String password) {
        internalPrefs.put(SHARED_DATABASE_PASSWORD, password);
    }

    public void setRememberPassword(boolean rememberPassword) {
        internalPrefs.putBoolean(SHARED_DATABASE_REMEMBER_PASSWORD, rememberPassword);
    }

    public void clearPassword() {
        internalPrefs.remove(SHARED_DATABASE_PASSWORD);
    }

    public void clear() throws BackingStoreException {
        internalPrefs.clear();
    }

    private Optional<String> getOptionalValue(String key) {
        return Optional.ofNullable(internalPrefs.get(key, null));
    }

    public static void clearAll() throws BackingStoreException {
        Preferences.userNodeForPackage(JabRefMain.class).parent().node(MAIN_NODE).clear();
    }
}
