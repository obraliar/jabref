package net.sf.jabref.remote;

import java.util.HashMap;
import java.util.Map;

public enum DBType {

    MYSQL("MySQL"),
    ORACLE("Oracle"),
    POSTGRESQL("PostgreSQL");

    private String type;

    private DBType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return this.type;
    }

    public Map<String, String> getStructure() {
        Map<String, String> structure = new HashMap<>();
        if (type.equals(DBType.MYSQL)) {
            structure.put(DBProcessor.REMOTE_ID, "INT");
            structure.put(DBProcessor.ENTRYTYPE, "VARCHAR");
        } else if (type.equals(DBType.POSTGRESQL)) {
            structure.put(DBProcessor.REMOTE_ID, "SERIAL");
            structure.put(DBProcessor.ENTRYTYPE, "VARCHAR");
        } else if (type.equals(DBType.ORACLE)) {
            structure.put(DBProcessor.REMOTE_ID, "NUMBER");
            structure.put(DBProcessor.ENTRYTYPE, "VARCHAR2");
        }
        return structure;
    }

}
