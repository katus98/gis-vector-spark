package com.katus.model;

import com.katus.constant.JoinType;
import com.katus.constant.SpatialRelationship;
import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.SpatialJoinArgs;
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
 * @author Keran Sun (katus)
 * @version 2.0, 2020-11-19
 */
@Slf4j
public class SpatialJoin {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        SpatialJoinArgs mArgs = SpatialJoinArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Spatial Join Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1());
        Layer joinLayer = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2());

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            targetLayer = targetLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            joinLayer = joinLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        targetLayer = targetLayer.index(14);
        joinLayer = joinLayer.index(14);
        JoinType joinType = JoinType.valueOf(mArgs.getJoinType().trim().toUpperCase());
        SpatialRelationship relationship = SpatialRelationship.valueOf(mArgs.getSpatialRelationship().trim().toUpperCase());

        log.info("Start Calculation");
        Layer layer = spatialJoin(targetLayer, joinLayer, joinType, relationship);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

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
                            key = tarFeature.getFid() + "#+";
                            attributes = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), joinFeature.getAttributes());
                        }
                    }
                    return new Tuple2<>(key, new Feature(tarFeature.getFid(), attributes, tarFeature.getGeometry()));
                })
                .cache();
        JavaPairRDD<String, Feature> joined = tempResult
                .filter(pairItem -> pairItem._1().endsWith("#+"))
                .mapToPair(pairItem -> new Tuple2<>(pairItem._2().getFid(), pairItem._2()));
        JavaPairRDD<String, Feature> disJoined = tempResult
                .filter(pairItem -> pairItem._1().endsWith("#-"))
                .reduceByKey((f1, f2) -> f1)
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
        if (joinType.equals(JoinType.ONE_TO_ONE)) {
            result = result.reduceByKey((f1, f2) -> f1).cache();
        } else {
            result = result.cache();
        }
        tempResult.unpersist();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
