package com.katus.util;

import com.katus.entity.Layer;
import com.katus.io.lg.LayerGenerator;
import com.katus.io.lg.RelationalDatabaseLayerGenerator;
import com.katus.io.lg.ShapeFileLayerGenerator;
import com.katus.io.lg.TextFileLayerGenerator;
import com.katus.io.reader.MySQLReader;
import com.katus.io.reader.PostgreSQLReader;
import com.katus.io.reader.RelationalDatabaseReader;
import com.katus.io.reader.TextFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author Keran Sun (katus)
 * @version 1.1, 2020-12-08
 */
@Slf4j
public final class InputUtil {
    public static Layer makeLayer(SparkSession ss, String source, Boolean hasHeader, Boolean isWkt,
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
        } else {
            sourceUri = "file://" + source;
        }
        LayerGenerator generator;
        if (sourceUri.startsWith("jdbc:")) {
            String dbType = sourceUri.substring(5, sourceUri.indexOf("://")).toLowerCase();
            String[] tables = source.split(",");
            RelationalDatabaseReader rdbReader;
            switch (dbType) {
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
        } else {
            TextFileReader reader = new TextFileReader(sourceUri, hasHeader, isWkt, geometryFields, separator, crs, charset);
            reader.setGeometryType(geometryType);
            generator = new TextFileLayerGenerator(ss, reader);
        }
        layer = generator.generate();
        return layer;
    }
}
