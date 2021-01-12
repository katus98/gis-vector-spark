package com.katus.model.at;

import com.katus.constant.TextRelationship;
import com.katus.entity.Feature;
import com.katus.entity.Layer;
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
        TextSelectorArgs mArgs = TextSelectorArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Field Text Selector Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), mArgs.getLayers().split(","), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Prepare calculation");
        String selectField = mArgs.getSelectField();
        TextRelationship relationShip = TextRelationship.valueOf(mArgs.getTextRelationship().trim().toUpperCase());
        String[] keywords = mArgs.getKeywords().split(",");

        log.info("Start Calculation");
        Layer layer = fieldTextSelect(targetLayer, selectField, relationShip, keywords);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer fieldTextSelect(Layer layer, String selectField, TextRelationship relationShip, String[] keywords) {
        if (keywords.length == 0 || (keywords.length == 1 && keywords[0].isEmpty())) return layer;
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
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
