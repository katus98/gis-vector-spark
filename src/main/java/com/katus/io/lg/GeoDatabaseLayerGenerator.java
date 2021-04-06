package com.katus.io.lg;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.io.reader.GeoDatabaseReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-23
 * @since 1.2
 */
public class GeoDatabaseLayerGenerator extends LayerGenerator {
    private final List<String> pathWithLayerNames;

    public GeoDatabaseLayerGenerator(SparkSession ss, String path, String[] layers) {
        super(ss);
        this.pathWithLayerNames = new ArrayList<>();
        for (String layer : layers) {
            if (layer.isEmpty()) continue;
            this.pathWithLayerNames.add(path + ":" + layer);
        }
    }

    public GeoDatabaseLayerGenerator(SparkSession ss, String[] pathWithLayerNames) {
        super(ss);
        this.pathWithLayerNames = new ArrayList<>(Arrays.asList(pathWithLayerNames));
    }

    @Override
    public Layer generate() {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaPairRDD<String, Feature> featuresWithInfo = jsc.parallelize(pathWithLayerNames)
                .flatMapToPair(pathWithLayerName -> {
                    GeoDatabaseReader reader = new GeoDatabaseReader(pathWithLayerName);
                    List<Tuple2<String, Feature>> result = new ArrayList<>();
                    Feature feature = reader.next();
                    String type = feature != null ? feature.getGeometry().getGeometryType() : "";
                    while (feature != null) {
                        result.add(new Tuple2<>(feature.getFid(), feature));
                        feature = reader.next();
                    }
                    feature = new Feature();
                    feature.setAttribute("fieldNames", reader.getFieldNames());
                    feature.setAttribute("crs", reader.getCrs());
                    feature.setAttribute("geometryType", type);
                    result.add(new Tuple2<>("###INFO###", feature));
                    return result.iterator();
                })
                .repartition(jsc.defaultParallelism())
                .cache();
        long featureCount = featuresWithInfo.count() - pathWithLayerNames.size();
        Map<String, Object> attributes = featuresWithInfo.filter(pairItem -> pairItem._1().equals("###INFO###")).first()._2().getAttributes();
        JavaPairRDD<String, Feature> features = featuresWithInfo.filter(pairItem -> !pairItem._1().equals("###INFO###")).cache();
        featuresWithInfo.unpersist();
        return Layer.create(features, (String[]) attributes.get("fieldNames"), (CoordinateReferenceSystem) attributes.get("crs"), (String) attributes.get("geometryType"), featureCount);
    }
}
