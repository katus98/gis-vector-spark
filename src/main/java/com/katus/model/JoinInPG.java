package com.katus.model;

import com.katus.constant.JoinType;
import com.katus.io.reader.CitusPostgreSQLReader;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.FieldJoinArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-21
 * @since 1.2
 */
@Slf4j
public class JoinInPG {
    public static void main(String[] args) throws IOException {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        FieldJoinArgs mArgs = FieldJoinArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Join In PG Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Load data");
        String tablePre1 = mArgs.getInput1();
        String[] tables1 = tablePre1.substring(tablePre1.indexOf(":") + 1).split(",");
        CitusPostgreSQLReader reader1 = new CitusPostgreSQLReader(InputUtil.connectionProp.getProperty("postgresql.url"), tables1,
                InputUtil.connectionProp.getProperty("postgresql.user"), InputUtil.connectionProp.getProperty("postgresql.password"),
                mArgs.getGeometryFields1().split(","), mArgs.getCrs1(), Boolean.parseBoolean(mArgs.getIsWkt1()), mArgs.getGeometryType1());
        Dataset<Row> tarDataset = reader1.read(ss);

        String tablePre2 = mArgs.getInput2();
        String[] tables2 = tablePre2.substring(tablePre2.indexOf(":") + 1).split(",");
        CitusPostgreSQLReader reader2 = new CitusPostgreSQLReader(InputUtil.connectionProp.getProperty("postgresql.url"), tables2,
                InputUtil.connectionProp.getProperty("postgresql.user"), InputUtil.connectionProp.getProperty("postgresql.password"),
                mArgs.getGeometryFields2().split(","), mArgs.getCrs2(), Boolean.parseBoolean(mArgs.getIsWkt2()), mArgs.getGeometryType2());
        Dataset<Row> joinDataset = reader2.read(ss);

        log.info("Start Calculation");
        Dataset<Row> tar = tarDataset.select("*", mArgs.getJoinFields1() + " as JOIN");
        Dataset<Row> join = joinDataset.select("*", mArgs.getJoinFields2() + " as JOIN");
        Dataset<Row> resultDataset = tar.join(join, JavaConverters.asScalaIteratorConverter(new ArrayList<>(Collections.singleton("JOIN")).iterator()).asScala().toSeq(), "left");
        JoinType joinType = JoinType.valueOf(mArgs.getJoinType().trim().toUpperCase());
        if (joinType.equals(JoinType.ONE_TO_ONE)) {
            JavaRDD<Row> rowRdd = resultDataset.toJavaRDD()
                    .mapToPair(row -> {
                        int id = row.getInt(row.fieldIndex("_id"));
                        return new Tuple2<>(id, row);
                    })
                    .reduceByKey((p1, p2) -> p1)
                    .map(Tuple2::_2);
            resultDataset = ss.createDataFrame(rowRdd, resultDataset.schema());
        }

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToDirByMap(resultDataset, Boolean.getBoolean(mArgs.getNeedHeader()));

        ss.close();
    }
}
