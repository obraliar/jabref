package net.sf.jabref.remote;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBHelperMySQLTest {

    private Connection connection;
    private DBHelper dbHelper;

    @Before
    public void setUp() {
        try {
            connection = DBConnector.getNewConnection(DBType.MYSQL, "localhost", "jabref", "travis", "");
            dbHelper = new DBHelper(connection);
            connection.createStatement().executeUpdate("CREATE TABLE `test` (`a` INT(1), `b` TEXT)");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetColumnNames() {
        Set<String> columns = dbHelper.getColumnNames("test");
        Assert.assertTrue(columns.remove("a"));
        Assert.assertTrue(columns.remove("b"));
        Assert.assertEquals(0, columns.size());
    }

    @Test
    public void testGetColumnNamesFailure() {
        Set<String> columns = dbHelper.getColumnNames("XXX");
        Assert.assertTrue(columns.isEmpty());
    }

    @Test
    public void testQuery() {
        try {
            String expectedValue = null;
            String actualValue = null;
            connection.createStatement().executeUpdate("INSERT INTO `test`(`a`, `b`) VALUES(0, 'test')");

            try (ResultSet expectedResultSet =
                    connection.createStatement().executeQuery("SELECT `b` FROM `test` WHERE `a` = 0");
                 ResultSet actualResultSet = dbHelper.query("SELECT `b` FROM `test` WHERE `a` = 0",
                                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                if (expectedResultSet.next()) {
                    actualValue = expectedResultSet.getString("b");
                }

                if (actualResultSet.next()) {
                    expectedValue = actualResultSet.getString("b");
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

        Set<String> actualSet = dbHelper.allToUpperCase(set);
        expectedSet.remove(actualSet);

        Assert.assertTrue(expectedSet.isEmpty());
    }

    @Test
    public void testClearTables() {
        try {
            connection.createStatement().executeUpdate("INSERT INTO `test`(`a`, `b`) VALUES(0, \"test\")");
            dbHelper.clearTables("test", "XXX");

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("SELECT `b` FROM `test` WHERE `a` = 0")) {
                Assert.assertFalse(resultSet.next());
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void clear() {
        try {
            connection.createStatement().executeUpdate("DROP TABLE `test`");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
