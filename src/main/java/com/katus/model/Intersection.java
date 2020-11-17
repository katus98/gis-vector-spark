package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.ClipArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
@Slf4j
public class Intersection {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ClipArgs mArgs = ClipArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Clip Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1());
        Layer extentLayer = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2());

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            targetLayer = targetLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            extentLayer = extentLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        targetLayer = targetLayer.index(14);
        extentLayer = extentLayer.index(14);

        log.info("Start Calculation");
        Layer layer = intersection(targetLayer, extentLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);

        ss.close();
    }

    public static Layer intersection(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata1 = tarIndLayer.getMetadata();
        LayerMetadata metadata2 = extIndLayer.getMetadata();
        String[] fieldNames = mergeFields(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result = tarIndLayer.join(extIndLayer)
                .mapToPair(pairItems -> {
                    Feature targetFeature = pairItems._2()._1();
                    Feature extentFeature = pairItems._2()._2();
                    Geometry geoTarget = targetFeature.getGeometry();
                    Geometry geoExtent = extentFeature.getGeometry();
                    Feature feature = null;
                    if (geoTarget.intersects(geoExtent)) {
                        String fid = targetFeature.getFid() + "#" + extentFeature.getFid();
                        LinkedHashMap<String, Object> attributes = mergeAttributes(fieldNames, targetFeature.getAttributes(), extentFeature.getAttributes());
                        Geometry inter = geoExtent.intersection(geoTarget);
                        feature = new Feature(fid, attributes, inter);
                    }
                    return new Tuple2<>(pairItems._1(), feature);
                })
                .filter(pairItem -> pairItem._2() != null)
                .cache();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }

    private static String[] mergeFields(String[] fieldNames1, String[] fieldNames2) {
        String[] fieldNames = new String[fieldNames1.length + fieldNames2.length];
        int i = 0;
        for (String field : fieldNames1) {
            fieldNames[i++] = "target_" + field;
        }
        for (String field : fieldNames2) {
            fieldNames[i++] = "extent_" + field;
        }
        return fieldNames;
    }

    private static LinkedHashMap<String, Object> mergeAttributes(String[] fieldNames, Map<String, Object> attr1, Map<String, Object> attr2) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            if (fieldName.startsWith("target_")) {
                attributes.put(fieldName, attr1.get(fieldName.substring(fieldName.indexOf("_") + 1)));
            } else {
                attributes.put(fieldName, attr2.get(fieldName.substring(fieldName.indexOf("_") + 1)));
            }
        }
        return attributes;
    }
}
