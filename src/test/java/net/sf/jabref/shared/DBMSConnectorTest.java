package net.sf.jabref.shared;

import java.sql.SQLException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DBMSConnectorTest {

    @Parameter
    public DBMSType dbmsType;


    @Parameters(name = "Test with {0} database system")
    public static Collection<DBMSType> getTestingDatabaseSystems() {
        return DBMSConnector.getAvailableDBMSTypes();
    }

    @Test
    public void testGetNewConnection() throws ClassNotFoundException, SQLException {
        TestConnectionData connectionData = TestConnector.getTestConnectionData(dbmsType);

        DBMSConnector.getNewConnection(dbmsType, connectionData.getHost(), connectionData.getDatabase(),
                connectionData.getUser(), connectionData.getPassord());
    }

    @Test(expected = SQLException.class)
    public void testGetNewConnectionFail() throws SQLException, ClassNotFoundException {
        DBMSConnector.getNewConnection(dbmsType, "XXXX", "XXXX", "XXXX", "XXXX");
    }
}
