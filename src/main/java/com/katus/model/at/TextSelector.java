package com.katus.model.at;

import com.katus.constant.TextRelationship;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.at.args.TextSelectorArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class TextSelector {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        TextSelectorArgs mArgs = new TextSelectorArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Field Text Selector Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput());

        log.info("Prepare calculation");
        Field selectField = inputLayer.getMetadata().getFieldByName(mArgs.getSelectField());
        TextRelationship relationShip = TextRelationship.valueOf(mArgs.getTextRelationship().trim().toUpperCase());
        String[] keywords = mArgs.getKeywords().split(",");

        log.info("Start Calculation");
        Layer layer = fieldTextSelect(inputLayer, selectField, relationShip, keywords);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer fieldTextSelect(Layer layer, Field selectField, TextRelationship relationShip, String[] keywords) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = null;
        switch (relationShip) {
            case EQUAL:
                result = layer.filter(pairItem -> {
                    String value = (String) pairItem._2().getAttribute(selectField);
                    for (String keyword : keywords) {
                        if (value.equals(keyword)) return true;
                    }
                    return false;
                }).cache();
                break;
            case CONTAIN:
                result = layer.filter(pairItem -> {
                    String value = (String) pairItem._2().getAttribute(selectField);
                    for (String keyword : keywords) {
                        if (value.contains(keyword)) return true;
                    }
                    return false;
                }).cache();
                break;
            case START_WITH:
                result = layer.filter(pairItem -> {
                    String value = (String) pairItem._2().getAttribute(selectField);
                    for (String keyword : keywords) {
                        if (value.startsWith(keyword)) return true;
                    }
                    return false;
                }).cache();
                break;
            case END_WITH:
                result = layer.filter(pairItem -> {
                    String value = (String) pairItem._2().getAttribute(selectField);
                    for (String keyword : keywords) {
                        if (value.endsWith(keyword)) return true;
                    }
                    return false;
                }).cache();
                break;
        }
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
