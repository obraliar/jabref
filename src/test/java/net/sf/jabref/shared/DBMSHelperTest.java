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
    public void setUp() throws ClassNotFoundException, SQLException {
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
    }

    @Parameters(name = "Test with {0} database system")
    public static Collection<DBMSType> getTestingDatabaseSystems() {
        return DBMSConnector.getAvailableDBMSTypes();
    }

    @Test
    public void testQuery() throws SQLException {
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
    }

    @After
    public void clear() throws SQLException {
        connection.createStatement().executeUpdate("DROP TABLE " + escape("TEST"));
    }

    public String escape(String expression) {
        return dbmsType.escape(expression);
    }

}
