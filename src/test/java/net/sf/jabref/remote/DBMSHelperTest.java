package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

    @Test
    public void testGetColumnNames() {
        Set<String> columns = dbmsHelper.allToUpperCase(dbmsHelper.getColumnNames("TEST"));
        Assert.assertTrue(columns.remove("A"));
        Assert.assertTrue(columns.remove("B"));
        Assert.assertEquals(0, columns.size());
    }

    @Test
    public void testGetColumnNamesFailure() {
        Set<String> columns = dbmsHelper.getColumnNames(escape("XXX"));
        Assert.assertTrue(columns.isEmpty());
    }

    @Test
    public void testQuery() {
        try {
            String expectedValue = null;
            String actualValue = null;
            connection.createStatement().executeUpdate(
                    "INSERT INTO " + escape("TEST") + "(" + escape("A") + ", " + escape("B") + ") VALUES(0, 'test')");

            try (ResultSet expectedResultSet = connection.createStatement().executeQuery(
                    "SELECT " + escape("B") + " FROM " + escape("TEST") + " WHERE " + escape("A") + " = 0");
                    ResultSet actualResultSet = dbmsHelper.query(
                            "SELECT " + escape("B") + " FROM " + escape("TEST") + " WHERE " + escape("A") + " = 0",
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                if (expectedResultSet.next()) {
                    actualValue = expectedResultSet.getString("B");
                }

                if (actualResultSet.next()) {
                    expectedValue = actualResultSet.getString("B");
                }

                Assert.assertNotNull(expectedValue);
                Assert.assertNotNull(actualValue);
                Assert.assertEquals(expectedValue, actualValue);

                expectedResultSet.close();
                actualResultSet.close();
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAllToUpperCase() {
        Set<String> set = new HashSet<>();
        set.add("aa");
        set.add("b0");

        Set<String> expectedSet = new HashSet<>();
        set.add("AA");
        set.add("B0");

        Set<String> actualSet = dbmsHelper.allToUpperCase(set);
        expectedSet.remove(actualSet);

        Assert.assertTrue(expectedSet.isEmpty());
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
