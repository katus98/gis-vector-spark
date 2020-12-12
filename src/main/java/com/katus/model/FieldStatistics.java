package com.katus.model;

import com.katus.constant.NumberType;
import com.katus.constant.StatisticalMethod;
import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.FieldStatisticsArgs;
import com.katus.util.AttributeUtil;
import com.katus.util.FieldUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class FieldStatistics {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        FieldStatisticsArgs mArgs = FieldStatisticsArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Field Statistics Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Prepare calculation");
        String[] categoryFields = mArgs.getCategoryFields().split(",");
        List<String> summaryFields = Arrays.asList(mArgs.getSummaryFields().split(","));
        List<NumberType> numberTypes = Arrays
                .stream(mArgs.getNumberTypes().split(","))
                .map(String::toUpperCase)
                .map(NumberType::valueOf)
                .collect(Collectors.toList());
        List<StatisticalMethod> statisticalMethods = Arrays
                .stream(mArgs.getStatisticalMethods().split(","))
                .map(String::toUpperCase)
                .map(StatisticalMethod::valueOf)
                .collect(Collectors.toList());
        if (statisticalMethods.contains(StatisticalMethod.MEAN)) {
            if (!statisticalMethods.contains(StatisticalMethod.SUM)) statisticalMethods.add(StatisticalMethod.SUM);
            if (!statisticalMethods.contains(StatisticalMethod.COUNT)) statisticalMethods.add(StatisticalMethod.COUNT);
        }
        if (!checkArgs(categoryFields, summaryFields, numberTypes, targetLayer.getMetadata().getFieldNames())) {
            String msg = "Field Statistics Args are not illegal, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Start Calculation");
        Layer layer = fieldStatistics(targetLayer, categoryFields, summaryFields, numberTypes, statisticalMethods);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, false);

        ss.close();
    }

    public static Layer fieldStatistics(Layer layer, String[] categoryFields, List<String> summaryFields, List<NumberType> numberTypes, List<StatisticalMethod> statisticalMethods) {
        LayerMetadata metadata = layer.getMetadata();
        String[] fieldNames = FieldUtil.initStatisticsFields(categoryFields, summaryFields, statisticalMethods);
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    StringBuilder keyBuilder = new StringBuilder("Statistics:");
                    for (String categoryField : categoryFields) {
                        keyBuilder.append(feature.getAttribute(categoryField)).append(",");
                    }
                    keyBuilder.deleteCharAt(keyBuilder.length() - 1);
                    LinkedHashMap<String, Object> attributes = AttributeUtil.initStatistics(feature.getAttributes(), categoryFields, summaryFields, numberTypes, statisticalMethods);
                    feature.setAttributes(attributes);
                    feature.setGeometry(null);
                    return new Tuple2<>(keyBuilder.toString(), feature);
                })
                .reduceByKey((feature1, feature2) -> {
                    LinkedHashMap<String, Object> attributes = AttributeUtil.statistic(feature1.getAttributes(), feature2.getAttributes(), summaryFields);
                    return new Feature(feature1.getFid(), attributes);
                }).mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    LinkedHashMap<String, Object> attributes = feature.getAttributes();
                    LinkedHashMap<String, Object> newAttrs = new LinkedHashMap<>();
                    Iterator<Map.Entry<String, Object>> it = attributes.entrySet().iterator();
                    int i = 0;
                    while (it.hasNext()) {
                        Map.Entry<String, Object> entry = it.next();
                        newAttrs.put(fieldNames[i++], entry.getValue());
                    }
                    feature.setAttributes(newAttrs);
                    return new Tuple2<>(pairItem._1(), feature);
                });
        if (statisticalMethods.contains(StatisticalMethod.MEAN)) {
            result = result.mapToPair(pairItem -> {
                Feature feature = pairItem._2();
                LinkedHashMap<String, Object> attributes = feature.getAttributes();
                for (String summaryField : summaryFields) {
                    long count = ((Number) attributes.get(summaryField + StatisticalMethod.COUNT.getFieldNamePostfix())).longValue();
                    double sum = ((Number) attributes.get(summaryField + StatisticalMethod.SUM.getFieldNamePostfix())).doubleValue();
                    attributes.put(summaryField + StatisticalMethod.MEAN.getFieldNamePostfix(), sum / count);
                }
                return pairItem;
            }).cache();
        } else {
            result = result.cache();
        }
        return Layer.create(result, fieldNames, metadata.getCrs(), "None", result.count());
    }

    private static boolean checkArgs(String[] categoryFields, List<String> summaryFields, List<NumberType> numberTypes, String[] fields) {
        boolean result = true;
        if (summaryFields.size() <= 0) result = false;
        if (summaryFields.size() != numberTypes.size()) result = false;
        List<String> fieldList = Arrays.asList(fields);
        for (String categoryField : categoryFields) {
            if (!fieldList.contains(categoryField)) {
                result = false;
                break;
            }
        }
        for (String summaryField : summaryFields) {
            if (!fieldList.contains(summaryField)) {
                result = false;
                break;
            }
        }
        return result;
    }
}
