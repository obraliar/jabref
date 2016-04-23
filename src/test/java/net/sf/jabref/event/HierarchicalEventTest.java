package net.sf.jabref.event;

import net.sf.jabref.bibtex.BibtexEntryAssert;
import net.sf.jabref.model.entry.BibEntry;

import com.google.common.eventbus.EventBus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HierarchicalEventTest {

    private EventBus eventBus;
    private TestHierarchicalEventListener testHierarchicalEventListener;


    @Before
    public void setUp() {
        eventBus = new EventBus();
        testHierarchicalEventListener = new TestHierarchicalEventListener();
        eventBus.register(testHierarchicalEventListener);
    }

    @Test
    public void testParentEntryOnChildReceivementOfAddEntryEvent() {
        BibEntry shouldBeBibEntry = new BibEntry();
        shouldBeBibEntry.setId("testkey3");
        AddEntryEvent aee = new AddEntryEvent(shouldBeBibEntry);
        eventBus.post(aee);

        BibtexEntryAssert.assertEquals(shouldBeBibEntry, testHierarchicalEventListener.getParentBibEntry());
        BibtexEntryAssert.assertEquals(shouldBeBibEntry, testHierarchicalEventListener.getBibEntry());
        BibtexEntryAssert.assertEquals(shouldBeBibEntry, aee.getEntry());
    }

    @Test
    public void testParentEntryOnChildReceivementOfChangeEntryEvent() {
        BibEntry shouldBeBibEntry = new BibEntry();
        shouldBeBibEntry.setId("testkey4");
        ChangeEntryEvent cee = new ChangeEntryEvent(shouldBeBibEntry);
        eventBus.post(cee);

        BibtexEntryAssert.assertEquals(shouldBeBibEntry, testHierarchicalEventListener.getParentBibEntry());
        BibtexEntryAssert.assertEquals(shouldBeBibEntry, testHierarchicalEventListener.getBibEntry());
        BibtexEntryAssert.assertEquals(shouldBeBibEntry, cee.getEntry());
    }

    @Test
    public void testChildEntryNullOnParentReceivementOfAddOrChangeEntryEvent() {
        BibEntry shouldBeBibEntry = new BibEntry();
        shouldBeBibEntry.setId("testkey5");
        AddOrChangeEntryEvent aocee = new AddOrChangeEntryEvent(shouldBeBibEntry);
        eventBus.post(aocee);

        BibtexEntryAssert.assertEquals(shouldBeBibEntry, testHierarchicalEventListener.getParentBibEntry());
        Assert.assertNull(testHierarchicalEventListener.getBibEntry());
        BibtexEntryAssert.assertEquals(shouldBeBibEntry, aocee.getEntry());
    }


}
