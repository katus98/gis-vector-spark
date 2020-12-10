package com.katus.io.reader;

import lombok.Getter;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-07
 * @since 1.1
 */
@Getter
public class PostgreSQLReader extends RelationalDatabaseReader {
    public PostgreSQLReader(String url, String[] tables, String username, String password) {
        super(url, tables, "org.postgresql.Driver", username, password);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String serialField) {
        super(url, tables, "org.postgresql.Driver", username, password, serialField);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String serialField, String[] geometryFields, String crs) {
        super(url, tables, "org.postgresql.Driver", username, password, serialField, geometryFields, crs);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String serialField, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        super(url, tables, "org.postgresql.Driver", username, password, serialField, geometryFields, crs, isWkt, geometryType);
    }
}
