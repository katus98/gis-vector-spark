package com.katus.io.reader;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-19
 * @since 1.1
 */
@Getter
public abstract class RelationalDatabaseReader implements Serializable {
    protected String url;
    protected String[] tables;
    protected String driver;
    protected String username;
    protected String password;
    protected String serialField;
    protected String[] geometryFields;
    protected String crs;
    protected Boolean isWkt;
    @Setter
    protected String geometryType;
    protected String[] fieldNames;
    @Setter
    protected Integer numPartitions = 16;
    @Setter
    protected Integer fetchSize = 2000;
    @Setter
    protected Boolean continueBatchOnError = true;
    @Setter
    protected Boolean pushDownPredicate = true;

    protected RelationalDatabaseReader(String url, String[] tables, String driver, String username, String password) {
        this(url, tables, driver, username, password, "");
    }

    protected RelationalDatabaseReader(String url, String[] tables, String driver, String username, String password, String serialField) {
        this(url, tables, driver, username, password, serialField, new String[0], "4326");
    }

    protected RelationalDatabaseReader(String url, String[] tables, String driver, String username, String password, String serialField, String[] geometryFields, String crs) {
        this(url, tables, driver, username, password, serialField, geometryFields, crs, true, "LineString");
    }

    protected RelationalDatabaseReader(String url, String[] tables, String driver, String username, String password, String serialField, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        this.url = url;
        this.tables = tables;
        this.driver = driver;
        this.username = username;
        this.password = password;
        this.serialField = serialField;
        this.geometryFields = geometryFields;
        this.crs = crs;
        this.isWkt = isWkt;
        this.geometryType = geometryType;
    }

    protected Dataset<Row> readMultipleTables(SparkSession ss) {
        List<Dataset<Row>> datasets = new ArrayList<>();
        for (String table : tables) {
            datasets.add(readSingleTable(ss, table));
        }
        return datasets.stream().reduce(Dataset::union).orElse(ss.emptyDataFrame());
    }

    protected Dataset<Row> readTable(SparkSession ss, String table, String url) {
        return ss.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("driver", driver)
                .option("user", username)
                .option("password", password)
                .option("fetchsize", fetchSize)   // 每次往返提取的行数 仅对读取生效
                .option("continueBatchOnError", continueBatchOnError)
                .option("pushDownPredicate", pushDownPredicate) // 默认请求下推
                .load();
    }

    protected Dataset<Row> readSingleTable(SparkSession ss, String table) {
        Dataset<Row> prefetch = ss.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("driver", driver)
                .option("user", username)
                .option("password", password)
                .option("fetchsize", fetchSize)   // 每次往返提取的行数 仅对读取生效
                .option("continueBatchOnError", continueBatchOnError)
                .option("pushDownPredicate", pushDownPredicate) // 默认请求下推
                .load();
        if (serialField == null || serialField.isEmpty() || serialField.equals("-")) return prefetch;
        Row[] bounds = (Row[]) prefetch.selectExpr("min(" + serialField + ") as min", "max(" + serialField + ") as max").collect();
        return ss.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("driver", driver)
                .option("partitionColumn", serialField)
                .option("lowerBound", ((Number) bounds[0].get(0)).longValue())
                .option("upperBound", ((Number) bounds[0].get(1)).longValue())
                .option("user", username)
                .option("password", password)
                .option("numPartitions", numPartitions)
                .option("fetchsize", fetchSize)   // 每次往返提取的行数 仅对读取生效
                .option("continueBatchOnError", continueBatchOnError)
                .option("pushDownPredicate", pushDownPredicate) // 默认请求下推
                .load();
    }

    public Dataset<Row> read(SparkSession ss) {
        Dataset<Row> df = readMultipleTables(ss);
        List<String> allFieldList = new ArrayList<>(Arrays.asList(df.columns()));
        for (String geometryField : geometryFields) {
            allFieldList.remove(geometryField);
        }
        this.fieldNames = new String[allFieldList.size()];
        allFieldList.toArray(this.fieldNames);
        return df;
    }
}
