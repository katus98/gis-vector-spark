package com.katus.util;

import com.katus.entity.Layer;
import com.katus.io.lg.*;
import com.katus.io.reader.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-20
 */
@Slf4j
public final class InputUtil {
    public static Layer makeLayer(SparkSession ss, String source, String[] layerNames, Boolean hasHeader, Boolean isWkt,
                                  String[] geometryFields, String separator, String crs, String charset,
                                  String geometryType, String serialField) throws Exception {
        Layer layer;
        String sourceUri;
        Properties connectionProp = new Properties();
        InputStream is = InputUtil.class.getResourceAsStream("/db.properties");
        connectionProp.load(is);
        if (source.startsWith("file://") || source.startsWith("hdfs://")) {
            sourceUri = source;
        } else if (source.startsWith("jdbc:")) {
            sourceUri = source.substring(0, source.lastIndexOf("/"));
            source = source.substring(sourceUri.length() + 1);
        } else if (source.startsWith("postgresql:")) {
            sourceUri = connectionProp.getProperty("postgresql.url");
            source = source.substring(11);
        } else if (source.startsWith("mysql:")) {
            sourceUri = connectionProp.getProperty("mysql.url");
            source = source.substring(6);
        } else if (source.startsWith("citus:")) {
            sourceUri = connectionProp.getProperty("postgresql.url") + ":citus";
            source = source.substring(6);
        } else {
            sourceUri = "file://" + source;
        }
        LayerGenerator generator;
        if (sourceUri.startsWith("jdbc:")) {
            String dbType = sourceUri.substring(5, sourceUri.indexOf("://")).toLowerCase();
            if (sourceUri.endsWith(":citus")) {
                sourceUri = sourceUri.substring(0, sourceUri.lastIndexOf(":"));
                dbType = "citus";
            }
            String[] tables = source.split(",");
            RelationalDatabaseReader rdbReader;
            switch (dbType) {
                case "citus":
                    rdbReader = new CitusPostgreSQLReader(sourceUri, tables, connectionProp.getProperty("postgresql.user"),
                            connectionProp.getProperty("postgresql.password"), geometryFields, crs, isWkt, geometryType);
                    break;
                case "mysql":
                    rdbReader = new MySQLReader(sourceUri, tables, connectionProp.getProperty(dbType + ".user"),
                            connectionProp.getProperty(dbType + ".password"), serialField, geometryFields, crs, isWkt, geometryType);
                    break;
                case "postgresql":
                    rdbReader = new PostgreSQLReader(sourceUri, tables, connectionProp.getProperty(dbType + ".user"),
                            connectionProp.getProperty(dbType + ".password"), serialField, geometryFields, crs, isWkt, geometryType);
                    break;
                default:
                    String msg = "Database: " + dbType + " Not Support!";
                    log.error(msg);
                    throw new RuntimeException(msg);
            }
            generator = new RelationalDatabaseLayerGenerator(ss, rdbReader);
        } else if (sourceUri.toLowerCase().endsWith(".shp")) {
            generator = new ShapeFileLayerGenerator(ss, source);
        } else if (sourceUri.toLowerCase().endsWith(".gdb") || sourceUri.toLowerCase().endsWith(".mdb")) {
            generator = new GeoDatabaseLayerGenerator(ss, source, layerNames);
        } else {
            TextFileReader reader = new TextFileReader(sourceUri, hasHeader, isWkt, geometryFields, separator, crs, charset);
            reader.setGeometryType(geometryType);
            generator = new TextFileLayerGenerator(ss, reader);
        }
        layer = generator.generate();
        return layer;
    }
}
