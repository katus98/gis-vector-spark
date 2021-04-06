package com.katus.model.dmt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.dmt.args.MergeArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

/**
 * @author Sun Katus
 * @version 1.3, 2020-12-14
 */
@Slf4j
public class Merge {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        MergeArgs mArgs = new MergeArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Merge Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer layer1 = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2());

        log.info("Dimension check");
        if (GeometryUtil.getDimensionOfGeomType(layer1.getMetadata().getGeometryType()) !=
                GeometryUtil.getDimensionOfGeomType(layer2.getMetadata().getGeometryType())) {
            String msg = "Two layers must have the same dimension, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Prepare calculation");
        if (mArgs.getCrs().isEmpty()) mArgs.setCrs(mArgs.getInput1().getCrs());
        else if (!mArgs.getCrs().equals(mArgs.getInput1().getCrs())) {
            layer1 = layer1.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getInput2().getCrs())) {
            layer2 = layer2.project(CrsUtil.getByCode(mArgs.getCrs()));
        }

        log.info("Start Calculation");
        Layer layer = merge(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer merge(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        String[] fieldNames = FieldUtil.mergeToLeast(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result1 = layer1.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            feature.setAttributes(AttributeUtil.merge(fieldNames, feature.getAttributes(), new HashMap<>()));
            return pairItem;
        });
        JavaPairRDD<String, Feature> result2 = layer2.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            feature.setAttributes(AttributeUtil.merge(fieldNames, new HashMap<>(), feature.getAttributes()));
            return pairItem;
        });
        JavaPairRDD<String, Feature> result = result1.union(result2).cache();
        long featureCount = metadata1.getFeatureCount() + metadata2.getFeatureCount();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), featureCount);
    }
}
