package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.DissolveArgs;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.Arrays;
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
        DissolveArgs mArgs = new DissolveArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Dissolve Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader = ReaderFactory.create(ss, mArgs.getInput());
        Layer inputLayer = reader.readToLayer();

        log.info("Prepare calculation");
        Field[] dissolveFields = Arrays
                .stream(mArgs.getDissolveFields().split(","))
                .filter(str -> !str.isEmpty())
                .map(inputLayer.getMetadata()::getFieldByName)
                .toArray(Field[]::new);

        log.info("Start Calculation");
        Layer layer = dissolve(inputLayer, dissolveFields);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer dissolve(Layer layer, Field[] dissolveFields) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    StringBuilder key = new StringBuilder("Dissolve:");
                    Feature feature = pairItem._2();
                    for (Field dissolveField : dissolveFields) {
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
                    LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
                    for (Field dissolveField : dissolveFields) {
                        attributes.put(dissolveField, feature.getAttribute(dissolveField));
                    }
                    feature.setAttributes(attributes);
                    return pairItem;
                })
                .cache();
        return Layer.create(result, dissolveFields, metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
