package com.katus.io.lg;

import com.katus.constant.GeomConstant;
import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.io.reader.RelationalDatabaseReader;
import com.katus.util.CrsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Keran Sun (katus)
 * @version 1.1, 2020-12-09
 * @since 1.1
 */
@Slf4j
public class RelationalDatabaseLayerGenerator extends LayerGenerator implements Serializable {
    private final RelationalDatabaseReader reader;

    public RelationalDatabaseLayerGenerator(SparkSession ss, RelationalDatabaseReader reader) {
        super(ss);
        this.reader = reader;
    }

    @Override
    public Layer generate() throws FactoryException {
        Dataset<Row> df = reader.read(ss);
        String[] fieldNames = reader.getFieldNames();
        String[] geometryFields = reader.getGeometryFields();
        LongAccumulator dataItemErrorCount = ss.sparkContext().longAccumulator("DataItemErrorCount");
        JavaPairRDD<String, Feature> features = df.rdd().toJavaRDD()
                .mapToPair(row -> {
                    try {
                        String[] geom = new String[geometryFields.length];
                        for (int i = 0; i < geometryFields.length; i++) {
                            geom[i] = row.getString(row.fieldIndex(geometryFields[i]));
                        }
                        Geometry geometry;
                        WKTReader wktReader = new WKTReader();
                        switch (geom.length) {
                            case 1:
                                if (reader.getIsWkt()) {
                                    geometry = wktReader.read(geom[0]);
                                } else {
                                    if (reader.getGeometryType().equalsIgnoreCase("Polygon")) {
                                        geometry = wktReader.read(String.format("POLYGON ((%s))", geom[0]));
                                    } else {
                                        geometry = wktReader.read(String.format("%s (%s)", reader.getGeometryType().toUpperCase(), geom[0]));
                                    }
                                }
                                break;
                            case 2:
                                geometry = wktReader.read(String.format("POINT (%s %s)", geom[0], geom[1]));
                                break;
                            case 4:
                                geometry = wktReader.read(String.format("LINESTRING (%s %s,%s %s)", geom[0], geom[1], geom[2], geom[3]));
                                break;
                            default:
                                geometry = GeomConstant.EMPTY_GEOM;
                        }
                        Feature feature = new Feature(geometry);
                        for (String fieldName : fieldNames) {
                            feature.setAttribute(fieldName, row.get(row.fieldIndex(fieldName)));
                        }
                        return new Tuple2<>(feature.getFid(), feature);
                    } catch (Exception e) {
                        dataItemErrorCount.add(1L);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .cache();
        long featureCount = features.count();
        log.warn("Data Item Error: " + dataItemErrorCount.count());
        String geometryType = features.first()._2().getGeometry().getGeometryType();
        reader.setGeometryType(geometryType);
        return Layer.create(features, reader.getFieldNames(), CrsUtil.getByCode(reader.getCrs()), geometryType, featureCount);
    }
}
