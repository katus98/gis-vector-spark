package com.katus.model;

import com.katus.io.reader.CitusPostgreSQLReader;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.SelectInPGArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-21
 * @since 1.2
 */
@Slf4j
public class SelectInPG {
    public static void main(String[] args) throws IOException {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        SelectInPGArgs mArgs = SelectInPGArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Select In PG Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Load data");
        String tablePre = mArgs.getInput();
        String[] tables = tablePre.substring(tablePre.indexOf(":") + 1).split(",");
        CitusPostgreSQLReader reader = new CitusPostgreSQLReader(InputUtil.connectionProp.getProperty("postgresql.url"), tables,
                InputUtil.connectionProp.getProperty("postgresql.user"), InputUtil.connectionProp.getProperty("postgresql.password"),
                new String[]{"wkt"}, "4490", true, "Polygon");
        Dataset<Row> dataset = reader.read(ss);

        log.info("Start Calculation");
        Dataset<Row> resultDataset = dataset.where(mArgs.getExpression());

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToDirByMap(resultDataset);

        ss.close();
    }
}
