package com.katus.io.lg;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.io.reader.ShapeFileReader;
import com.katus.util.CrsUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-11
 */
public class ShapeFileLayerGenerator extends LayerGenerator {
    private final String fileURI;

    public ShapeFileLayerGenerator(SparkSession ss, String fileURI) {
        super(ss);
        this.fileURI = fileURI;
    }

    @Override
    public Layer generate() throws FactoryException {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaPairRDD<String, Feature> featuresWithInfo = jsc.parallelize(Collections.singletonList(fileURI))
                .flatMapToPair(filename -> {
                    ShapeFileReader reader = new ShapeFileReader(filename);
                    List<Tuple2<String, Feature>> result = new ArrayList<>();
                    Feature feature = reader.next();
                    String type = feature != null ? feature.getGeometry().getGeometryType() : "";
                    while (feature != null) {
                        result.add(new Tuple2<>(feature.getFid(), feature));
                        feature = reader.next();
                    }
                    reader.close();
                    feature = new Feature();
                    feature.setAttribute("fieldNames", reader.getFieldNames());
                    feature.setAttribute("crs", reader.getCrs());
                    feature.setAttribute("geometryType", type);
                    result.add(new Tuple2<>("###INFO###", feature));
                    return result.iterator();
                })
                .repartition(jsc.defaultParallelism())
                .cache();
        long featureCount = featuresWithInfo.count() - 1;
        Map<String, Object> attributes = featuresWithInfo.filter(pairItem -> pairItem._1().equals("###INFO###")).first()._2().getAttributes();
        JavaPairRDD<String, Feature> features = featuresWithInfo.filter(pairItem -> !pairItem._1().equals("###INFO###")).cache();
        featuresWithInfo.unpersist();
        CoordinateReferenceSystem crs = CrsUtil.getByESRIWkt((String) attributes.get("crs"));
        return Layer.create(features, (String[]) attributes.get("fieldNames"), crs, (String) attributes.get("geometryType"), featureCount);
    }
}
