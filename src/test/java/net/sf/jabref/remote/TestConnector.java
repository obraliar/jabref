package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class TestConnector {

    public static DBMSType currentConnectionType;


    public static Connection getTestConnection(DBMSType dbmsType) throws ClassNotFoundException, SQLException {
        String user = "root";
        String password = "";
        String database = "jabref";

        if (dbmsType == DBMSType.POSTGRESQL) {
            user = "postgres";
        } else if (dbmsType == DBMSType.ORACLE) {
            user = "travis";
            password = "travis";
            database = "xe";
        }
        currentConnectionType = dbmsType;

        return DBMSConnector.getNewConnection(dbmsType, "localhost", database, user, password);
    }

    public static Set<DBMSType> getAvailableDBMSTypes() {
        Set<DBMSType> dbmsTypes = new HashSet<>();

        for (DBMSType dbms : DBMSType.values()) {
            try {
                Class.forName(dbms.getDriverClassPath());
                dbmsTypes.add(dbms);
            } catch (ClassNotFoundException e) {
                // In case that the driver is not available do not perform tests for this system.
                System.out.println(dbms + " driver not available. Skipping tests for this system...");
            }
        }
        return dbmsTypes;
    }
}