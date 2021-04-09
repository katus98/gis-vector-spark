package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.ConvexHullArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-14
 */
@Slf4j
public class ConvexHull {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ConvexHullArgs mArgs = new ConvexHullArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Convex Hull Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput());

        log.info("Start Calculation");
        Layer layer = convexHull(inputLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer convexHull(Layer layer) {
        LayerMetadata metadata = layer.getMetadata();
        String geometryType = metadata.getGeometryType().equalsIgnoreCase("Point") ? "Point" : "Polygon";
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    Geometry ch = feature.getGeometry().convexHull();
                    feature.setGeometry(ch);
                    return new Tuple2<>(pairItem._1(), feature);
                })
                .cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), geometryType, metadata.getFeatureCount());
    }
}
