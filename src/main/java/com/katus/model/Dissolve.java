package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.DissolveArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
@Slf4j
public class Dissolve {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        DissolveArgs mArgs = DissolveArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Dissolve Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType());

        log.info("Prepare calculation");
        String[] dissolveFields = mArgs.getDissolveFields().split(",");

        log.info("Start Calculation");
        Layer layer = dissolve(targetLayer, dissolveFields);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);

        ss.close();
    }

    public static Layer dissolve(Layer layer, String[] dissolveFields) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    StringBuilder key = new StringBuilder();
                    Feature feature = pairItem._2();
                    for (String dissolveField : dissolveFields) {
                        key.append(feature.getAttribute(dissolveField)).append(",");
                    }
                    key.deleteCharAt(key.length() - 1);
                    return new Tuple2<>(key.toString(), feature);
                })
                .reduceByKey((feature1, feature2) -> {
                    Geometry union = feature1.getGeometry().union(feature2.getGeometry());
                    Feature feature = new Feature(union);
                    for (String dissolveField : dissolveFields) {
                        feature.setAttribute(dissolveField, feature1.getAttribute(dissolveField));
                    }
                    return feature;
                })
                .cache();
        return Layer.create(result, dissolveFields, metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
