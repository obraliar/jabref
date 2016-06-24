package net.sf.jabref.remote;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DBConnectorTest {

    DBConnector connector;

    @Before
    public void setUp() {
        connector = new DBConnector();
    }

    @Test
    public void testGetNewMySQLConnection() {
        try {
            connector.getNewConnection(DBType.MYSQL, "localhost", "jabref", "travis", "");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetNewPostgreSQLConnection() {
        try {
            connector.getNewConnection(DBType.POSTGRESQL, "localhost", "jabref", "postgres", "");

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetNewMySQLConnectionFail() {
        try {
            connector.getNewConnection(DBType.MYSQL, "XXXX", "XXXX", "XXXX", "XXXX");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertEquals(0, (((SQLException) e).getErrorCode()));
        }
    }

    @Test
    public void testGetNewPostgreSQLConnectionFail() {
        try {
            connector.getNewConnection(DBType.POSTGRESQL, "XXXX", "XXXX", "XXXX", "XXXX");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertEquals(0, (((SQLException) e).getErrorCode()));
        }
    }

    // TODO Oracle

    @Test
    public void testGetDefaultPort() {
        Assert.assertEquals(3306, connector.getDefaultPort(DBType.MYSQL));
        Assert.assertEquals(5432, connector.getDefaultPort(DBType.POSTGRESQL));
        Assert.assertEquals(1521, connector.getDefaultPort(DBType.ORACLE));
        Assert.assertEquals(-1, connector.getDefaultPort(null));
    }

}
