package com.katus.model.at;

import com.katus.constant.FieldMark;
import com.katus.constant.GeometryType;
import com.katus.constant.StatisticalMethod;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.at.args.StatisticsArgs;
import com.katus.util.AttributeUtil;
import com.katus.util.FieldUtil;
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
public class Statistics {

    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        StatisticsArgs mArgs = new StatisticsArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Field Statistics Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader = ReaderFactory.create(ss, mArgs.getInput());
        Layer inputLayer = reader.readToLayer();

        log.info("Prepare calculation");
        Field[] categoryFields = Arrays
                .stream(mArgs.getCategoryFields().split(","))
                .filter(str -> !str.isEmpty())
                .map(inputLayer.getMetadata()::getFieldByName)
                .toArray(Field[]::new);
        Field[] summaryFields = Arrays
                .stream(mArgs.getSummaryFields().split(","))
                .map(inputLayer.getMetadata()::getFieldByName)
                .filter(field -> {
                    if (!Number.class.isAssignableFrom(field.getType().getClazz())) {
                        log.warn("Field " + field.getName() + " is not a numerical field, has been taken out!");
                        return false;
                    } else return true;
                })
                .toArray(Field[]::new);
        List<StatisticalMethod> statMethodList = Arrays
                .stream(mArgs.getStatisticalMethods().split(","))
                .map(String::toUpperCase)
                .map(StatisticalMethod::valueOf)
                .collect(Collectors.toList());
        if (statMethodList.contains(StatisticalMethod.MEAN)) {
            if (!statMethodList.contains(StatisticalMethod.SUM)) statMethodList.add(StatisticalMethod.SUM);
            if (!statMethodList.contains(StatisticalMethod.COUNT)) statMethodList.add(StatisticalMethod.COUNT);
        }

        log.info("Start Calculation");
        Layer layer = fieldStatistics(inputLayer, categoryFields, summaryFields, statMethodList);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, false);

        ss.close();
    }

    public static Layer fieldStatistics(Layer layer, Field[] categoryFields, Field[] summaryFields, List<StatisticalMethod> statMethodList) {
        LayerMetadata metadata = layer.getMetadata();
        Field[] fields = FieldUtil.initStatisticsFields(categoryFields, summaryFields, statMethodList);
        JavaPairRDD<String, Feature> result = layer
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    StringBuilder keyBuilder = new StringBuilder("Statistics:");
                    for (Field categoryField : categoryFields) {
                        keyBuilder.append(feature.getAttribute(categoryField)).append(",");
                    }
                    keyBuilder.deleteCharAt(keyBuilder.length() - 1);
                    LinkedHashMap<Field, Object> attributes = AttributeUtil.initStatistics(fields, categoryFields, summaryFields, statMethodList, feature.getAttributes());
                    feature.setAttributes(attributes);
                    feature.setGeometry(null);
                    return new Tuple2<>(keyBuilder.toString(), feature);
                })
                .reduceByKey((feature1, feature2) -> {
                    LinkedHashMap<Field, Object> attributes = AttributeUtil.statistic(feature1.getAttributes(), feature2.getAttributes());
                    return new Feature(feature1.getFid(), attributes);
                });
        if (statMethodList.contains(StatisticalMethod.MEAN)) {
            result = result.mapToPair(pairItem -> {
                Feature feature = pairItem._2();
                LinkedHashMap<Field, Object> attributes = feature.getAttributes();
                for (Field summaryField : summaryFields) {
                    Field countField = summaryField.copy(), sumField = summaryField.copy();
                    countField.setMark(FieldMark.STAT_COUNT);
                    sumField.setMark(FieldMark.STAT_SUM);
                    long count = ((Number) attributes.get(countField)).longValue();
                    double sum = ((Number) attributes.get(sumField)).doubleValue();
                    Field meanField = FieldUtil.getFieldByNameAndMark(fields, summaryField.getName(), FieldMark.STAT_MEAN);
                    attributes.put(meanField, sum / count);
                }
                return pairItem;
            }).cache();
        } else {
            result = result.cache();
        }
        return Layer.create(result, fields, metadata.getCrs(), GeometryType.NONE, result.count());
    }
}
