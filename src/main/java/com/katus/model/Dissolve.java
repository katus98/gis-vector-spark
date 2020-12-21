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

import java.util.LinkedHashMap;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
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
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Prepare calculation");
        String[] dissolveFields = mArgs.getDissolveFields().split(",");
        if (dissolveFields[0].isEmpty()) dissolveFields = new String[0];

        log.info("Start Calculation");
        Layer layer = dissolve(targetLayer, dissolveFields);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer dissolve(Layer layer, String[] dissolveFields) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    StringBuilder key = new StringBuilder("Dissolve:");
                    Feature feature = pairItem._2();
                    for (String dissolveField : dissolveFields) {
                        key.append(feature.getAttribute(dissolveField)).append(",");
                    }
                    if (dissolveFields.length > 0) key.deleteCharAt(key.length() - 1);
                    return new Tuple2<>(key.toString(), feature);
                })
                .reduceByKey((feature1, feature2) -> {
                    Geometry union = feature1.getGeometry().union(feature2.getGeometry());
                    feature1.setGeometry(union);
                    return feature1;
                })
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
                    for (String dissolveField : dissolveFields) {
                        attributes.put(dissolveField, feature.getAttribute(dissolveField));
                    }
                    feature.setAttributes(attributes);
                    return pairItem;
                })
                .cache();
        return Layer.create(result, dissolveFields, metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
