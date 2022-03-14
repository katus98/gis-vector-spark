package com.katus.model.dmt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.dmt.args.MergeArgs;
import com.katus.util.AttributeUtil;
import com.katus.util.CrsUtil;
import com.katus.util.FieldUtil;
import com.katus.util.SparkUtil;
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
        Reader reader1 = ReaderFactory.create(ss, mArgs.getInput1());
        Reader reader2 = ReaderFactory.create(ss, mArgs.getInput2());
        Layer layer1 = reader1.readToLayer();
        Layer layer2 = reader2.readToLayer();

        log.info("Dimension check");
        if (layer1.getMetadata().getGeometryType().getDimension() != layer2.getMetadata().getGeometryType().getDimension()) {
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
        Field[] fields = FieldUtil.mergeToLeast(metadata1.getFields(), metadata2.getFields());
        JavaPairRDD<String, Feature> result1 = layer1.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            feature.setAttributes(AttributeUtil.merge(fields, feature.getAttributes(), new HashMap<>()));
            return pairItem;
        });
        JavaPairRDD<String, Feature> result2 = layer2.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            feature.setAttributes(AttributeUtil.merge(fields, new HashMap<>(), feature.getAttributes()));
            return pairItem;
        });
        JavaPairRDD<String, Feature> result = result1.union(result2).cache();
        long featureCount = metadata1.getFeatureCount() + metadata2.getFeatureCount();
        return Layer.create(result, fields, metadata1.getCrs(), metadata1.getGeometryType(), featureCount);
    }
}
