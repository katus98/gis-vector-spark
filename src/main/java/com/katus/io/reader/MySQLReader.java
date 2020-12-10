package com.katus.io.reader;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-08
 * @since 1.1
 */
public class MySQLReader extends RelationalDatabaseReader {
    public MySQLReader(String url, String[] tables, String username, String password) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String serialField) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, serialField);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String serialField, String[] geometryFields, String crs) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, serialField, geometryFields, crs);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String serialField, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, serialField, geometryFields, crs, isWkt, geometryType);
    }
}
