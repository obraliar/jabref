package net.sf.jabref.event;

<<<<<<< HEAD
import net.sf.jabref.model.FieldChange;
=======
import net.sf.jabref.event.location.EntryEventLocation;
>>>>>>> Implementation of shared database support (base system).
import net.sf.jabref.model.entry.BibEntry;

/**
 * <code>FieldChangedEvent</code> is fired when a field of <code>BibEntry</code> has been modified, removed or added.
 */
public class FieldChangedEvent extends EntryChangedEvent {

    private final String fieldName;
    private final String newValue;

    /**
     * @param bibEntry Affected BibEntry object
     * @param fieldName Name of field which has been changed
     * @param newValue new field value
     */
    public FieldChangedEvent(BibEntry bibEntry, String fieldName, String newValue) {
        super(bibEntry);
        this.fieldName = fieldName;
        this.newValue = newValue;
    }

<<<<<<< HEAD
    public FieldChangedEvent(FieldChange fieldChange) {
        this(fieldChange.getEntry(), fieldChange.getField(), fieldChange.getNewValue());
=======
    /**
     * @param bibEntry Affected BibEntry object
     * @param fieldName Name of field which has been changed
     * @param newValue new field value
     * @param location location Location affected by this event
     */
    public FieldChangedEvent(BibEntry bibEntry, String fieldName, String newValue, EntryEventLocation location) {
        super(bibEntry, location);
        this.fieldName = fieldName;
        this.newValue = newValue;
>>>>>>> Implementation of shared database support (base system).
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getNewValue() {
        return newValue;
    }

}
