package com.katus.model;

import com.katus.constant.NumberRelationship;
import com.katus.constant.NumberType;
import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.FieldNumericalSelectorArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Method;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-19
 */
@Slf4j
public class FieldNumericalSelector {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        FieldNumericalSelectorArgs mArgs = FieldNumericalSelectorArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Field Numerical Selector Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType());

        log.info("Prepare calculation");
        String selectField = mArgs.getSelectField();
        NumberRelationship relationship = NumberRelationship.getBySymbol(mArgs.getNumberRelationship());
        NumberType numberType = NumberType.valueOf(mArgs.getNumberType().trim().toUpperCase());
        Method valueOfMethod = Class.forName(numberType.getClassFullName()).getMethod("valueOf", String.class);
        Number threshold = (Number) valueOfMethod.invoke(null, mArgs.getThreshold());

        log.info("Start Calculation");
        Layer layer = fieldNumericalSelect(targetLayer, selectField, relationship, numberType, threshold);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer fieldNumericalSelect(Layer layer, String selectField, NumberRelationship relationShip, NumberType numberType, Number threshold) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.filter(pairItem -> {
            Class<?> clazz = Class.forName(numberType.getClassFullName());
            Object value = pairItem._2().getAttribute(selectField);
            Number number;
            if (value instanceof String) {
                String valueStr = (String) value;
                Method valueOfMethod = clazz.getMethod("valueOf", String.class);
                number = (Number) valueOfMethod.invoke(null, valueStr);
            } else if (value instanceof Number) {
                number = (Number) value;
            } else {
                return false;
            }
            Method compareToMethod = clazz.getMethod("compareTo", clazz);
            Integer compareResult = (Integer) compareToMethod.invoke(number, threshold);
            return relationShip.check(compareResult);
        }).cache();
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
