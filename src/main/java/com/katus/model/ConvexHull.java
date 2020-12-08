package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.ConvexHullArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Keran Sun (katus)
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class ConvexHull {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ConvexHullArgs mArgs = ConvexHullArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Convex Hull Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Start Calculation");
        Layer layer = convexHull(targetLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

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
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), geometryType, result.count());
    }
}
