package com.katus.io.reader;

import lombok.Getter;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-12-07
 * @since 1.1
 */
@Getter
public class PostgreSQLReader extends RelationalDatabaseReader {
    public PostgreSQLReader(String url, String[] tables) {
        super(url, tables, "org.postgresql.Driver", "postgres", "postgres");
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password) {
        super(url, tables, "org.postgresql.Driver", username, password);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String idField) {
        super(url, tables, "org.postgresql.Driver", username, password, idField);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String idField, String[] geometryFields, String crs) {
        super(url, tables, "org.postgresql.Driver", username, password, idField, geometryFields, crs);
    }

    public PostgreSQLReader(String url, String[] tables, String username, String password, String idField, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        super(url, tables, "org.postgresql.Driver", username, password, idField, geometryFields, crs, isWkt, geometryType);
    }
}
