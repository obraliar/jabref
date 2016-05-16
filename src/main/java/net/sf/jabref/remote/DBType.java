package net.sf.jabref.remote;


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
}
