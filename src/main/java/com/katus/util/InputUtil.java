package com.katus.util;

import com.katus.entity.Layer;
import com.katus.io.lg.LayerGenerator;
import com.katus.io.lg.ShapeFileLayerGenerator;
import com.katus.io.lg.TextFileLayerGenerator;
import com.katus.io.reader.TextFileReader;
import org.apache.spark.sql.SparkSession;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-16
 */
public final class InputUtil {
    public static Layer makeLayer(SparkSession ss, String filename) throws Exception {
        Layer layer;
        String fileURI;
        if (filename.startsWith("file://") || filename.startsWith("hdfs://")) {
            fileURI = filename;
        } else {
            fileURI = "file://" + filename;
        }
        LayerGenerator generator;
        if (fileURI.toLowerCase().endsWith(".shp")) {
            generator = new ShapeFileLayerGenerator(ss, filename);
        } else {
            TextFileReader reader = new TextFileReader(fileURI);
            generator = new TextFileLayerGenerator(ss, reader);
        }
        layer = generator.generate();
        return layer;
    }

    public static Layer makeLayer(SparkSession ss, String filename, Boolean hasHeader,
                                                      Boolean isWkt, String[] geometryFields, String separator,
                                                      String crs, String charset) throws Exception {
        Layer layer;
        String fileURI;
        if (filename.startsWith("file://") || filename.startsWith("hdfs://")) {
            fileURI = filename;
        } else {
            fileURI = "file://" + filename;
        }
        LayerGenerator generator;
        if (fileURI.toLowerCase().endsWith(".shp")) {
            generator = new ShapeFileLayerGenerator(ss, filename);
        } else {
            TextFileReader reader = new TextFileReader(fileURI, hasHeader, isWkt, geometryFields, separator, crs, charset);
            generator = new TextFileLayerGenerator(ss, reader);
        }
        layer = generator.generate();
        return layer;
    }
}
