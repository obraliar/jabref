package net.sf.jabref.shared;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.shared.DBMSConnector;
import net.sf.jabref.shared.DBMSHelper;
import net.sf.jabref.shared.DBMSProcessor;
import net.sf.jabref.shared.DBMSType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DBMSProcessorTest {

    private static Connection connection;
    private DBMSProcessor dbmsProcessor;
    private DBMSHelper dbmsHelper;

    @Parameter
    public DBMSType dbmsType;


    @Before
    public void setUp() {

        // Get only one connection for each parameter
        if (TestConnector.currentConnectionType != dbmsType) {
            try {
                connection = TestConnector.getTestConnection(dbmsType);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
        dbmsHelper = new DBMSHelper(connection);
        dbmsProcessor = new DBMSProcessor(dbmsHelper, dbmsType);
        dbmsProcessor.setUpSharedDatabase();

    }

    @Parameters(name = "Test with {0} database system")
    public static Collection<DBMSType> getTestingDatabaseSystems() {
        return DBMSConnector.getAvailableDBMSTypes();
    }

    @Test
    public void testCheckBaseIntegrity() {
        Assert.assertTrue(dbmsProcessor.checkBaseIntegrity());
        clear();
        Assert.assertFalse(dbmsProcessor.checkBaseIntegrity());
    }

    @Test
    public void testSetUpSharedDatabase() {
        clear();
        dbmsProcessor.setUpSharedDatabase();
        Assert.assertTrue(dbmsProcessor.checkBaseIntegrity());
    }

    @Test
    public void testInsertEntry() {

        BibEntry realEntry = new BibEntry();
        realEntry.setType("inproceedings");
        realEntry.setField("author", "Wirthlin, Michael J and Hutchings, Brad L and Gilson, Kent L");
        realEntry.setField("title", "The nano processor: a low resource reconfigurable processor");
        realEntry.setField("booktitle", "FPGAs for Custom Computing Machines, 1994. Proceedings. IEEE Workshop on");
        realEntry.setField("year", "1994");
        realEntry.setCiteKey("nanoproc1994");
        realEntry.setSharedID(1);

        dbmsProcessor.insertEntry(realEntry);

        BibEntry emptyEntry = new BibEntry();
        emptyEntry.setSharedID(1);
        dbmsProcessor.insertEntry(emptyEntry); // does not insert, due to same sharedID.

        try (ResultSet resultSet = selectFrom("ENTRY")) {

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(realEntry.getType(), resultSet.getString("ENTRYTYPE"));
            Assert.assertEquals(realEntry.getFieldOptional("author").get(), resultSet.getString("AUTHOR"));
            Assert.assertEquals(realEntry.getFieldOptional("title").get(), resultSet.getString("TITLE"));
            Assert.assertEquals(realEntry.getFieldOptional("booktitle").get(), resultSet.getString("BOOKTITLE"));
            Assert.assertEquals(realEntry.getFieldOptional("year").get(), resultSet.getString("YEAR"));
            Assert.assertEquals(realEntry.getCiteKey(), resultSet.getString("BIBTEXKEY"));
            Assert.assertFalse(resultSet.next());

        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRemoveEntry() {
        BibEntry bibEntry = getBibEntryExample();
        dbmsProcessor.insertEntry(bibEntry);
        dbmsProcessor.removeEntry(bibEntry);

        try (ResultSet resultSet = selectFrom("ENTRY")) {
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetSharedEntries() {
        BibEntry bibEntry = getBibEntryExampleWithEmptyFields();

        dbmsProcessor.insertEntry(bibEntry);

        List<BibEntry> expectedEntries = Arrays.asList(bibEntry);
        List<BibEntry> actualEntries = dbmsProcessor.getSharedEntries();

        Assert.assertEquals(expectedEntries, actualEntries);
    }

    @Test
    public void testGetSharedMetaData() {
        insertMetaData("databaseType", "bibtex;");
        insertMetaData("protectedFlag", "true;");
        insertMetaData("saveActions", "enabled;\nauthor[capitalize,html_to_latex]\ntitle[title_case]\n;");
        insertMetaData("saveOrderConfig", "specified;author;false;title;false;year;true;");

        Map<String, String> expectedMetaData = getMetaDataExample();
        Map<String, String> actualMetaData = dbmsProcessor.getSharedMetaData();

        Assert.assertEquals(expectedMetaData, actualMetaData);

    }

    @Test
    public void testSetSharedMetaData() {
        Map<String, String> expectedMetaData = getMetaDataExample();
        dbmsProcessor.setSharedMetaData(expectedMetaData);

        Map<String, String> actualMetaData = dbmsProcessor.getSharedMetaData();

        Assert.assertEquals(expectedMetaData, actualMetaData);
    }

    @Test
    public void testEscape() {

        if (dbmsType == DBMSType.MYSQL) {
            Assert.assertEquals("`TABLE`", dbmsProcessor.escape("TABLE"));
            Assert.assertEquals("`TABLE`", DBMSProcessor.escape("TABLE", dbmsType));
        } else if (dbmsType == DBMSType.POSTGRESQL) {
            Assert.assertEquals("TABLE", dbmsProcessor.escape("TABLE"));
            Assert.assertEquals("TABLE", DBMSProcessor.escape("TABLE", dbmsType));
        } else if (dbmsType == DBMSType.ORACLE) {
            Assert.assertEquals("\"TABLE\"", dbmsProcessor.escape("TABLE"));
            Assert.assertEquals("\"TABLE\"", DBMSProcessor.escape("TABLE", dbmsType));
        }

        Assert.assertEquals("TABLE", DBMSProcessor.escape("TABLE", null));
    }

    private Map<String, String> getMetaDataExample() {
        Map<String, String> expectedMetaData = new HashMap<>();

        expectedMetaData.put("databaseType", "bibtex;");
        expectedMetaData.put("protectedFlag", "true;");
        expectedMetaData.put("saveActions", "enabled;\nauthor[capitalize,html_to_latex]\ntitle[title_case]\n;");
        expectedMetaData.put("saveOrderConfig", "specified;author;false;title;false;year;true;");

        return expectedMetaData;
    }

    private BibEntry getBibEntryExampleWithEmptyFields() {
        BibEntry bibEntry = new BibEntry();
        bibEntry.setField("author", "Author");
        bibEntry.setField("title", "");
        bibEntry.setField("year", "");
        bibEntry.setSharedID(1);
        return bibEntry;
    }

    private BibEntry getBibEntryExample() {
        BibEntry bibEntry = new BibEntry();
        bibEntry.setType("book");
        bibEntry.setField("author", "Wirthlin, Michael J");
        bibEntry.setField("title", "The nano processor");
        return bibEntry;
    }

    private ResultSet selectFrom(String table) {
        try {
            return connection.createStatement().executeQuery("SELECT * FROM " + escape(table));
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
            return null;
        }
    }

    // Oracle does not support multiple tuple insertion in one INSERT INTO command.
    // Therefore this function was defined to improve the readability and to keep the code short.
    private void insertMetaData(String key, String value) {
        try {
            connection.createStatement().executeUpdate("INSERT INTO " + escape("METADATA") + "("
                    + escape("KEY") + ", " + escape("VALUE") + ") VALUES("
                    + escapeValue(key) + ", " + escapeValue(value) + ")");
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    private String escape(String expression) {
        return dbmsProcessor.escape(expression);
    }

    private String escapeValue(String value) {
        return "'" + value + "'";
    }

    @After
    public void clear() {
        try {
            if ((dbmsType == DBMSType.MYSQL) || (dbmsType == DBMSType.POSTGRESQL)) {
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + escape("ENTRY"));
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + escape("METADATA"));
            } else if (dbmsType == DBMSType.ORACLE) {
                connection.createStatement().executeUpdate(
                            "BEGIN\n" +
                            "EXECUTE IMMEDIATE 'DROP TABLE " + escape("ENTRY") + "';\n" +
                            "EXECUTE IMMEDIATE 'DROP TABLE " + escape("METADATA") + "';\n" +
                            "EXECUTE IMMEDIATE 'DROP SEQUENCE " + escape("ENTRY_SEQ") + "';\n" +
                            "EXCEPTION\n" +
                            "WHEN OTHERS THEN\n" +
                            "IF SQLCODE != -942 THEN\n" +
                            "RAISE;\n" +
                            "END IF;\n" +
                            "END;");
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}
