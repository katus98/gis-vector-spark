package com.katus.io.lg;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.io.reader.RelationalDatabaseReader;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Sun Katus
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
                        Geometry geometry = GeometryUtil.getGeometryFromText(geom, reader.getIsWkt(), reader.getGeometryType());
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
        String geometryType = featureCount > 0 ? features.first()._2().getGeometry().getGeometryType() : "EMPTY";
        reader.setGeometryType(geometryType);
        return Layer.create(features, reader.getFieldNames(), CrsUtil.getByCode(reader.getCrs()), geometryType, featureCount);
    }
}
