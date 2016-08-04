package net.sf.jabref.shared;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.shared.exception.OfflineLockException;
import net.sf.jabref.shared.exception.SharedEntryNotPresentException;

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
        dbmsProcessor = new DBMSProcessor(new DBMSHelper(connection), dbmsType);
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
        BibEntry expectedEntry = getBibEntryExample();

        dbmsProcessor.insertEntry(expectedEntry);

        BibEntry emptyEntry = new BibEntry();
        emptyEntry.setSharedID(1);
        dbmsProcessor.insertEntry(emptyEntry); // does not insert, due to same sharedID.

        Map<String, String> actualFieldMap = new HashMap<>();

        try (ResultSet entryResultSet = selectFrom("ENTRY")) {
            Assert.assertTrue(entryResultSet.next());
            Assert.assertEquals(1, entryResultSet.getInt("SHARED_ID"));
            Assert.assertEquals("inproceedings", entryResultSet.getString("TYPE"));
            Assert.assertEquals(1, entryResultSet.getInt("VERSION"));
            Assert.assertFalse(entryResultSet.next());

            try (ResultSet fieldResultSet = selectFrom("FIELD")) {
                while (fieldResultSet.next()) {
                    actualFieldMap.put(fieldResultSet.getString("NAME"), fieldResultSet.getString("VALUE"));
                }
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }

        Map<String, String> expectedFieldMap = expectedEntry.getFieldMap();

        Assert.assertEquals(expectedFieldMap, actualFieldMap);
    }

    @Test
    public void testUpdateEntry() {
        BibEntry expectedEntry = getBibEntryExample();

        dbmsProcessor.insertEntry(expectedEntry);

        expectedEntry.setType("book");
        expectedEntry.setField("author", "Michael J and Hutchings");
        expectedEntry.setField("customField", "custom value");
        expectedEntry.clearField("booktitle");

        try {
            dbmsProcessor.updateEntry(expectedEntry);
        } catch (OfflineLockException | SharedEntryNotPresentException e) {
            Assert.fail(e.getMessage());
        }

        Optional<BibEntry> actualEntryOptional = dbmsProcessor.getSharedEntry(expectedEntry.getSharedID());

        if (actualEntryOptional.isPresent()) {
            Assert.assertEquals(expectedEntry, actualEntryOptional.get());
        } else {
            Assert.fail();
        }
    }

    @Test(expected = SharedEntryNotPresentException.class)
    public void testUpdateNotExistingEntry() throws SharedEntryNotPresentException {
        BibEntry expectedEntry = getBibEntryExample();

        try {
            dbmsProcessor.updateEntry(expectedEntry);
        } catch (SharedEntryNotPresentException e) {
            throw e; // should happen
        } catch (OfflineLockException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(expected = OfflineLockException.class)
    public void testUpdateNewerEntry() throws OfflineLockException {
        BibEntry bibEntry = getBibEntryExample();

        dbmsProcessor.insertEntry(bibEntry);

        bibEntry.setVersion(0); // simulate older version
        bibEntry.setField("year", "1993");

        try {
            dbmsProcessor.updateEntry(bibEntry);
        } catch (SharedEntryNotPresentException e) {
            Assert.fail(e.getMessage());
        } catch (OfflineLockException e) {
            throw e; // should happen
        }
    }

    @Test
    public void testUpdateEqualEntry() {
        BibEntry expectedBibEntry = getBibEntryExample();

        dbmsProcessor.insertEntry(expectedBibEntry);

        expectedBibEntry.setVersion(0); // simulate older version

        try {
            dbmsProcessor.updateEntry(expectedBibEntry);
        } catch (SharedEntryNotPresentException | OfflineLockException e) {
            Assert.fail(e.getMessage());
        }

        Optional<BibEntry> actualBibEntryOptional = dbmsProcessor.getSharedEntry(expectedBibEntry.getSharedID());

        if (actualBibEntryOptional.isPresent()) {
            Assert.assertEquals(expectedBibEntry, actualBibEntryOptional.get());
        } else {
            Assert.fail();
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
    public void testGetSharedEntry() {
        BibEntry expectedBibEntry = getBibEntryExampleWithEmptyFields();

        dbmsProcessor.insertEntry(expectedBibEntry);

        Optional<BibEntry> actualBibEntryOptional = dbmsProcessor.getSharedEntry(expectedBibEntry.getSharedID());

        if (actualBibEntryOptional.isPresent()) {
            Assert.assertEquals(expectedBibEntry, actualBibEntryOptional.get());
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testGetNotExistingSharedEntry() {
        Optional<BibEntry> actualBibEntryOptional = dbmsProcessor.getSharedEntry(1);
        Assert.assertFalse(actualBibEntryOptional.isPresent());
    }

    @Test
    public void testGetSharedIDVersionMapping() throws OfflineLockException, SharedEntryNotPresentException {
        BibEntry firstEntry = getBibEntryExample();
        BibEntry secondEntry = getBibEntryExample();

        dbmsProcessor.insertEntry(firstEntry);
        dbmsProcessor.insertEntry(secondEntry);
        dbmsProcessor.updateEntry(secondEntry);

        Map<Integer, Integer> expectedIDVersionMap = new HashMap<>();
        expectedIDVersionMap.put(firstEntry.getSharedID(), 1);
        expectedIDVersionMap.put(secondEntry.getSharedID(), 2);

        Map<Integer, Integer> actualIDVersionMap = dbmsProcessor.getSharedIDVersionMapping();

        Assert.assertEquals(expectedIDVersionMap, actualIDVersionMap);

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
        bibEntry.setType("inproceedings");
        bibEntry.setField("author", "Wirthlin, Michael J and Hutchings, Brad L and Gilson, Kent L");
        bibEntry.setField("title", "The nano processor: a low resource reconfigurable processor");
        bibEntry.setField("booktitle", "FPGAs for Custom Computing Machines, 1994. Proceedings. IEEE Workshop on");
        bibEntry.setField("year", "1994");
        bibEntry.setCiteKey("nanoproc1994");
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
        return dbmsType.escape(expression);
    }

    private String escapeValue(String value) {
        return "'" + value + "'";
    }

    @After
    public void clear() {
        try {
            if ((dbmsType == DBMSType.MYSQL) || (dbmsType == DBMSType.POSTGRESQL)) {
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + escape("FIELD"));
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + escape("ENTRY"));
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS " + escape("METADATA"));
            } else if (dbmsType == DBMSType.ORACLE) {
                connection.createStatement().executeUpdate(
                            "BEGIN\n" +
                            "EXECUTE IMMEDIATE 'DROP TABLE " + escape("FIELD") + "';\n" +
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
