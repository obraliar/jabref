package net.sf.jabref.shared;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import net.sf.jabref.shared.DBMSConnector;
import net.sf.jabref.shared.DBMSHelper;
import net.sf.jabref.shared.DBMSProcessor;
import net.sf.jabref.shared.DBMSType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DBMSHelperTest {

    private Connection connection;
    private DBMSHelper dbmsHelper;

    @Parameter
    public DBMSType dbmsType;


    @Before
    public void setUp() {
        try {
            connection = TestConnector.getTestConnection(dbmsType);
            dbmsHelper = new DBMSHelper(connection);
            connection.createStatement().executeUpdate(
                    "CREATE TABLE " + escape("TEST") + " (" + escape("A") + " INT, " + escape("B") + " "
                            + (dbmsType == DBMSType.ORACLE ? "CLOB" : "TEXT") + ")");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Parameters(name = "Test with {0} database system")
    public static Collection<DBMSType> getTestingDatabaseSystems() {
        return DBMSConnector.getAvailableDBMSTypes();
    }

    @After
    public void clear() {
        try {
            connection.createStatement().executeUpdate("DROP TABLE " + escape("TEST"));
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    public String escape(String expression) {
        return DBMSProcessor.escape(expression, dbmsType);
    }

}
