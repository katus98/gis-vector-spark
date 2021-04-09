package com.katus.model.at;

import com.katus.constant.FieldType;
import com.katus.constant.NumberRelationship;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.at.args.NumericalSelectorArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Method;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class NumericalSelector {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        NumericalSelectorArgs mArgs = new NumericalSelectorArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Field Numerical Selector Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput());

        log.info("Prepare calculation");
        Field selectField = inputLayer.getMetadata().getFieldByName(mArgs.getSelectField());
        if (selectField.getType().equals(FieldType.TEXT)) {
            String msg = "Field Numerical Selector does not support TEXT field, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }
        NumberRelationship relationship = NumberRelationship.getBySymbol(mArgs.getNumberRelationship());
        Number threshold;
        if (!Number.class.isAssignableFrom(selectField.getType().getClazz())) {
            log.warn("The selected field is a DATE/DATETIME field!");
            threshold = Long.valueOf(mArgs.getThreshold());
        } else {
            Method valueOfMethod = selectField.getType().getClazz().getMethod("valueOf", String.class);
            threshold = (Number) valueOfMethod.invoke(null, mArgs.getThreshold());
        }

        log.info("Start Calculation");
        Layer layer = fieldNumericalSelect(inputLayer, selectField, relationship, threshold);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer fieldNumericalSelect(Layer layer, Field selectField, NumberRelationship relationShip, Number threshold) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.filter(pairItem -> {
            Number number = pairItem._2().getAttributeToNumber(selectField);
            if (number == null) return false;
            Class<?> clazz = selectField.getType().getClazz();
            Method compareToMethod = clazz.getMethod("compareTo", clazz);
            Integer compareResult = (Integer) compareToMethod.invoke(number, threshold);
            return relationShip.check(compareResult);
        }).cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
