package com.katus.model.at;

import com.katus.constant.JoinType;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.at.args.JoinArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-14
 */
@Slf4j
public class Join {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        JoinArgs mArgs = new JoinArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Field Join Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer baseLayer = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer joinLayer = InputUtil.makeLayer(ss, mArgs.getInput2());

        log.info("Prepare calculation");
        JoinType joinType = JoinType.valueOf(mArgs.getJoinType().trim().toUpperCase());
        String[] joinFields1 = mArgs.getJoinFields1().split(",");
        String[] joinFields2 = mArgs.getJoinFields2().split(",");

        log.info("Start Calculation");
        Layer layer = fieldJoin(baseLayer, joinLayer, joinType, joinFields1, joinFields2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer fieldJoin(Layer baseLayer, Layer joinLayer, JoinType joinType, String[] joinFields1, String[] joinFields2) {
        LayerMetadata metadata1 = baseLayer.getMetadata();
        LayerMetadata metadata2 = joinLayer.getMetadata();
        String[] fieldNames = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
        int fieldNum = Math.min(joinFields1.length, joinFields2.length);
        JavaPairRDD<String, Feature> result1 = baseLayer.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            StringBuilder builder = new StringBuilder("Join:");
            for (int i = 0; i < fieldNum; i++) {
                builder.append(feature.getAttribute(joinFields1[i])).append(",");
            }
            if (fieldNum > 0) builder.deleteCharAt(builder.length() - 1);
            return new Tuple2<>(builder.toString(), feature);
        });
        JavaPairRDD<String, Feature> result2 = joinLayer.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            StringBuilder builder = new StringBuilder("Join:");
            for (int i = 0; i < fieldNum; i++) {
                builder.append(feature.getAttribute(joinFields2[i])).append(",");
            }
            if (fieldNum > 0) builder.deleteCharAt(builder.length() - 1);
            return new Tuple2<>(builder.toString(), feature);
        });
        JavaPairRDD<String, Feature> result = result1.leftOuterJoin(result2)
                .mapToPair(leftPairItems -> {
                    Feature tarFeature = leftPairItems._2()._1();
                    LinkedHashMap<String, Object> attributes;
                    if (leftPairItems._2()._2().isPresent()) {
                        Feature joinFeature = leftPairItems._2()._2().get();
                        attributes = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), joinFeature.getAttributes());
                    } else {
                        attributes = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), new HashMap<>());
                    }
                    tarFeature.setAttributes(attributes);
                    return new Tuple2<>(tarFeature.getFid(), tarFeature);
                });
        Long featureCount;
        if (joinType.equals(JoinType.ONE_TO_ONE)) {
            result = result.reduceByKey((f1, f2) -> f1).cache();
            featureCount = metadata1.getFeatureCount();
        } else {
            result = result.cache();
            featureCount = result.count();
        }
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), featureCount);
    }
}
