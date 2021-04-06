package com.katus.model.dmt;

import com.katus.constant.JoinType;
import com.katus.constant.SpatialRelationship;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.dmt.args.SpatialJoinArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-11
 */
@Slf4j
public class SpatialJoin {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        SpatialJoinArgs mArgs = new SpatialJoinArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Spatial Join Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer baseLayer = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer joinLayer = InputUtil.makeLayer(ss, mArgs.getInput2());

        log.info("Prepare calculation");
        if (!mArgs.getInput1().getCrs().equals(mArgs.getInput2().getCrs())) {
            joinLayer = joinLayer.project(CrsUtil.getByCode(mArgs.getInput1().getCrs()));
        }
        baseLayer = baseLayer.index();
        joinLayer = joinLayer.index();
        JoinType joinType = JoinType.valueOf(mArgs.getJoinType().trim().toUpperCase());
        SpatialRelationship relationship = SpatialRelationship.valueOf(mArgs.getSpatialRelationship().trim().toUpperCase());

        log.info("Start Calculation");
        Layer layer = spatialJoin(baseLayer, joinLayer, joinType, relationship);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer spatialJoin(Layer targetLayer, Layer joinLayer, JoinType joinType, SpatialRelationship relationship) {
        LayerMetadata metadata1 = targetLayer.getMetadata();
        LayerMetadata metadata2 = joinLayer.getMetadata();
        String[] fieldNames = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> tempResult = targetLayer.leftOuterJoin(joinLayer)
                .mapToPair(leftPairItems -> {
                    Feature tarFeature = leftPairItems._2()._1();
                    String key = tarFeature.getFid() + "#-";
                    LinkedHashMap<String, Object> attributes = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), new HashMap<>());
                    if (leftPairItems._2()._2().isPresent()) {
                        Feature joinFeature = leftPairItems._2()._2().get();
                        Method spatialMethod = Geometry.class.getMethod(relationship.getMethodName(), Geometry.class);
                        Boolean isSatisfied = (Boolean) spatialMethod.invoke(tarFeature.getGeometry(), joinFeature.getGeometry());
                        if (isSatisfied) {
                            key = tarFeature.getFid() + "#" + joinFeature.getFid() + "#+";
                            attributes = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), joinFeature.getAttributes());
                        }
                    }
                    return new Tuple2<>(key, new Feature(tarFeature.getFid(), attributes, tarFeature.getGeometry()));
                })
                .reduceByKey((f1, f2) -> f1)
                .cache();
        JavaPairRDD<String, Feature> joined = tempResult
                .filter(pairItem -> pairItem._1().endsWith("#+"))
                .mapToPair(pairItem -> new Tuple2<>(pairItem._2().getFid(), pairItem._2()));
        JavaPairRDD<String, Feature> disJoined = tempResult
                .filter(pairItem -> pairItem._1().endsWith("#-"))
                .mapToPair(pairItem -> new Tuple2<>(pairItem._2().getFid(), pairItem._2()));
        JavaPairRDD<String, Feature> result = joined.fullOuterJoin(disJoined)
                .mapToPair(fullPairItems -> {
                    if (fullPairItems._2()._1().isPresent()) {
                        Feature joinedFeature = fullPairItems._2()._1().get();
                        return new Tuple2<>(fullPairItems._1(), joinedFeature);
                    } else {
                        Feature disJoinedFeature = fullPairItems._2()._2().get();
                        return new Tuple2<>(fullPairItems._1(), disJoinedFeature);
                    }
                });
        Long featureCount;
        if (joinType.equals(JoinType.ONE_TO_ONE)) {
            result = result.reduceByKey((f1, f2) -> f1).cache();
            featureCount = metadata1.getFeatureCount();
        } else {
            result = result.cache();
            featureCount = result.count();
        }
        tempResult.unpersist();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), featureCount);
    }
}
