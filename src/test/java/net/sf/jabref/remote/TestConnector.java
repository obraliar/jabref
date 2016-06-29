package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.SQLException;

public class TestConnector {

    public static Connection getTestConnection(DBType dbType) throws ClassNotFoundException, SQLException {
        String user = "";
        String password = "";
        String database = "jabref";

        if (dbType == DBType.MYSQL) {
            user = "travis";
        } else if (dbType == DBType.POSTGRESQL) {
            user = "admir";
            password = "q1w2e3r4";
        } else if (dbType == DBType.ORACLE) {
            user = "admir";
            password = "q1w2e3r4";
            database = "xe";
        }

        return DBConnector.getNewConnection(dbType, "localhost", database, user, password);

    }
}
