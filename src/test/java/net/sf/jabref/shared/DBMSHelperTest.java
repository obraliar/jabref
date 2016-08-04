package net.sf.jabref.shared;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

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
            StringBuilder createTableQuery = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(escape("TEST"))
                    .append(" (")
                    .append(escape("A"))
                    .append(" INT, ")
                    .append(escape("B"))
                    .append((dbmsType == DBMSType.ORACLE ? " CLOB" : " TEXT"))
                    .append(")");

            connection = TestConnector.getTestConnection(dbmsType);
            dbmsHelper = new DBMSHelper(connection);
            connection.createStatement().executeUpdate(createTableQuery.toString());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Parameters(name = "Test with {0} database system")
    public static Collection<DBMSType> getTestingDatabaseSystems() {
        return DBMSConnector.getAvailableDBMSTypes();
    }

    @Test
    public void testQuery() {
        try {
            StringBuilder insertQuery = new StringBuilder()
                    .append("INSERT INTO ")
                    .append(escape("TEST"))
                    .append("(")
                    .append(escape("A"))
                    .append(", ")
                    .append(escape("B"))
                    .append(") VALUES(0, 'b')");
            connection.createStatement().executeUpdate(insertQuery.toString());


            StringBuilder selectQuery = new StringBuilder()
                    .append("SELECT * FROM ")
                    .append(escape("TEST"));

            try (ResultSet resultSet = dbmsHelper.query(selectQuery.toString())) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(0, resultSet.getInt("A"));
                Assert.assertEquals("b", resultSet.getString("B"));
                Assert.assertFalse(resultSet.next());
            }

        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
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
        return dbmsType.escape(expression);
    }

}
