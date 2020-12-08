package com.katus.io.reader;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-12-08
 * @since 1.1
 */
public class MySQLReader extends RelationalDatabaseReader {
    protected MySQLReader(String url, String[] tables) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", "root", "root");
    }

    protected MySQLReader(String url, String[] tables, String username, String password) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String idField) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, idField);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String idField, String[] geometryFields, String crs) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, idField, geometryFields, crs);
    }

    public MySQLReader(String url, String[] tables, String username, String password, String idField, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        super(url, tables, "com.mysql.cj.jdbc.Driver", username, password, idField, geometryFields, crs, isWkt, geometryType);
    }
}
