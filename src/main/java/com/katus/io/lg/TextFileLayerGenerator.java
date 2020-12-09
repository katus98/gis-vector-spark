package com.katus.io.lg;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.io.reader.TextFileReader;
import com.katus.util.CrsUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-13
 */
public class TextFileLayerGenerator extends LayerGenerator implements Serializable {
    private final TextFileReader reader;

    public TextFileLayerGenerator(SparkSession ss, TextFileReader reader) {
        super(ss);
        this.reader = reader;
    }

    @Override
    public Layer generate() throws FactoryException {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaPairRDD<String, Feature> features = jsc.textFile(reader.getFileURI())
                .repartition(jsc.defaultParallelism())
                .filter(line -> !reader.getHasHeader() || !line.startsWith(reader.getFieldNames()[0]))
                .mapToPair(line -> {
                    String[] items = line.split(reader.getSeparator());
                    String fid = UUID.randomUUID().toString();
                    LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
                    Geometry geometry;
                    String[] fieldNames = reader.getFieldNames();
                    String[] geom = new String[4];
                    int index;
                    for (int i = 0; i < items.length; i++) {
                        index = TextFileReader.isGeomField(fieldNames[i], reader.getGeometryFields());
                        if (index >= 0) {
                            geom[index] = items[i];
                            continue;
                        }
                        attributes.put(fieldNames[i], items[i]);
                    }
                    WKTReader wktReader = new WKTReader();
                    if (reader.getIsWkt()) {   // wkt
                        geometry = wktReader.read(geom[0]);
                    } else if (reader.getGeometryFields().length == 2) {   // lat, lon
                        geometry = wktReader.read(String.format("POINT (%s %s)", geom[0], geom[1]));
                    } else if (reader.getGeometryFields().length == 4) {   // OD: sLat, sLon, eLat, eLon
                        geometry = wktReader.read(String.format("LINESTRING (%s %s,%s %s)", geom[0], geom[1], geom[2], geom[3]));
                    } else {   // Coordinate of points in one field. Need geometry type, default LineString.
                        if (reader.getGeometryType().equalsIgnoreCase("Polygon")) {
                            geometry = wktReader.read(String.format("POLYGON ((%s))", geom[0]));
                        } else {
                            geometry = wktReader.read(String.format("%s (%s)", reader.getGeometryType().toUpperCase(), geom[0]));
                        }
                    }
                    return new Tuple2<>(fid, new Feature(fid, attributes, geometry));
                });
        features.cache();
        long featureCount = features.count();
        String geometryType = features.first()._2().getGeometry().getGeometryType();
        features.unpersist();
        reader.setGeometryType(geometryType);
        return Layer.create(features, getFields(reader.getFieldNames(), reader.getGeometryFields()), CrsUtil.getByCode(reader.getCrs()), geometryType, featureCount);
    }

    private static String[] getFields(String[] allFields, String[] geomFields) {
        String[] fields = new String[allFields.length - geomFields.length];
        for (int i = 0, j = 0; i < allFields.length; i++) {
            if (TextFileReader.isGeomField(allFields[i], geomFields) == -1) {
                fields[j++] = allFields[i];
            }
        }
        return fields;
    }
}
