/*  Copyright (C) 2003-2016 JabRef contributors.
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
package net.sf.jabref.remote;

/**
 * Enumerates all supported database systems (DBS) by JabRef.
 */
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

    public static DBType fromString(String type) {
        if (type.equals(DBType.MYSQL.toString())) {
            return DBType.MYSQL;
        } else if (type.equals(DBType.ORACLE.toString())) {
            return DBType.ORACLE;
        } else if (type.equals(DBType.POSTGRESQL.toString())) {
            return DBType.POSTGRESQL;
        }
        return null;
    }
}
