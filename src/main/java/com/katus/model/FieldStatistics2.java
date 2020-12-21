package com.katus.model;

import com.katus.constant.StatisticalMethod;
import com.katus.io.reader.PostgreSQLReader;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.FieldStatistics2Args;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-20
 */
@Slf4j
public class FieldStatistics2 {
    public static void main(String[] args) throws IOException {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        FieldStatistics2Args mArgs = FieldStatistics2Args.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Field Statistics2 Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        String tablePre = mArgs.getInput();
        String[] tables = tablePre.substring(tablePre.indexOf(":") + 1).split(",");
        PostgreSQLReader reader = new PostgreSQLReader(InputUtil.connectionProp.getProperty("postgresql.url"), tables, InputUtil.connectionProp.getProperty("postgresql.user"),
                InputUtil.connectionProp.getProperty("postgresql.password"), "", mArgs.getGeometryFields().split(","), mArgs.getCrs(),
                Boolean.parseBoolean(mArgs.getIsWkt()), mArgs.getGeometryType());
        Dataset<Row> data = reader.read(ss);
        List<StatisticalMethod> statisticalMethods = Arrays
                .stream(mArgs.getStatisticalMethods().split(","))
                .map(String::toUpperCase)
                .map(StatisticalMethod::valueOf)
                .collect(Collectors.toList());
        String[] categoryFields = mArgs.getCategoryFields().isEmpty() ? new String[0] : mArgs.getCategoryFields().split(",");
        String[] summaryFields = mArgs.getSummaryFields().isEmpty() ? new String[0] : mArgs.getSummaryFields().split(",");
        List<Tuple2<String, String>> exprList = new ArrayList<>();
        for (String summaryField : summaryFields) {
            for (StatisticalMethod statisticalMethod : statisticalMethods) {
                exprList.add(new Tuple2<>(summaryField, statisticalMethod.getFunName()));
            }
        }
        RelationalGroupedDataset groupedDataset;
        switch (categoryFields.length) {
            case 0:
                groupedDataset = data.groupBy();
                break;
            case 1:
                groupedDataset = data.groupBy(categoryFields[0]);
                break;
            default:
                String[] categoryFields2 = new String[categoryFields.length - 1];
                System.arraycopy(categoryFields, 1, categoryFields2, 0, categoryFields.length - 1);
                groupedDataset = data.groupBy(categoryFields[0], categoryFields2);
        }
        Dataset<Row> df;
        if (exprList.isEmpty()) {
            df = groupedDataset.count();
        } else {
            df = groupedDataset.agg(exprList.get(0), JavaConverters.asScalaIteratorConverter(exprList.subList(1, exprList.size()).iterator()).asScala().toSeq());
        }
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(df);

        ss.close();
    }
}