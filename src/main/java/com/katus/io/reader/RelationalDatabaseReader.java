package com.katus.io.reader;

import com.katus.entity.LayerMetadata;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.io.InputInfo;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import com.katus.util.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-13
 * @since 2.0
 */
@Slf4j
public abstract class RelationalDatabaseReader extends Reader {

    protected RelationalDatabaseReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    protected Layer readToLayer(RelationalDatabaseReaderHelper readerHelper, Dataset<Row> df) {
        Field[] fields = readerHelper.getFields();
        List<String> geometryFieldNameList = readerHelper.getGeometryFieldNameList();
        LongAccumulator dataItemErrorCount = ss.sparkContext().longAccumulator("DataItemErrorCount");
        JavaPairRDD<String, Feature> features = df.rdd().toJavaRDD()
                .mapToPair(row -> {
                    try {
                        String[] geom = new String[geometryFieldNameList.size()];
                        for (int i = 0; i < geom.length; i++) {
                            geom[i] = row.getString(row.fieldIndex(geometryFieldNameList.get(i)));
                        }
                        Geometry geometry = GeometryUtil.getGeometryFromText(geom, inputInfo.getGeometryFieldFormat(), readerHelper.getGeometryType());
                        Feature feature = new Feature(geometry);
                        for (Field field : fields) {
                            feature.setAttribute(field, row.get(row.fieldIndex(field.getName())));
                        }
                        return new Tuple2<>(feature.getFid(), feature);
                    } catch (Exception e) {
                        dataItemErrorCount.add(1L);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .cache();
        long featureCount = features.count();
        log.info("Data Item Error: " + dataItemErrorCount.count());
        return new Layer(features, new LayerMetadata(readerHelper, featureCount));
    }

    protected Dataset<Row> readSmallTable(RelationalDatabaseReaderHelper readerHelper, String source, String table) {
        return ss.read()
                .format("jdbc")
                .option("url", source)
                .option("dbtable", table)
                .option("driver", readerHelper.driver)
                .option("user", readerHelper.username)
                .option("password", readerHelper.password)
                .option("fetchsize", readerHelper.fetchSize)   // 每次往返提取的行数 仅对读取生效
                .option("continueBatchOnError", readerHelper.continueBatchOnError)
                .option("pushDownPredicate", readerHelper.pushDownPredicate) // 默认请求下推
                .load();
    }

    protected Dataset<Row> readSingleTable(RelationalDatabaseReaderHelper readerHelper, String table) {
        Dataset<Row> prefetch = readSmallTable(readerHelper, readerHelper.source, table);
        if (inputInfo.getSerialField().isEmpty()) {
            return prefetch;
        }
        Row[] bounds = (Row[]) prefetch.selectExpr("min(" + inputInfo.getSerialField() + ") as min",
                "max(" + inputInfo.getSerialField() + ") as max").collect();
        return ss.read()
                .format("jdbc")
                .option("url", readerHelper.source)
                .option("dbtable", table)
                .option("driver", readerHelper.driver)
                .option("partitionColumn", inputInfo.getSerialField())
                .option("lowerBound", ((Number) bounds[0].get(0)).longValue())
                .option("upperBound", ((Number) bounds[0].get(1)).longValue())
                .option("user", readerHelper.username)
                .option("password", readerHelper.password)
                .option("numPartitions", readerHelper.numPartitions)
                .option("fetchsize", readerHelper.fetchSize)   // 每次往返提取的行数 仅对读取生效
                .option("continueBatchOnError", readerHelper.continueBatchOnError)
                .option("pushDownPredicate", readerHelper.pushDownPredicate) // 默认请求下推
                .load();
    }

    protected Dataset<Row> readMultipleTables(RelationalDatabaseReaderHelper readerHelper) {
        List<Dataset<Row>> datasets = new ArrayList<>();
        for (String table : readerHelper.tables) {
            datasets.add(readSingleTable(readerHelper, table));
        }
        return datasets.stream().reduce(Dataset::union).orElse(ss.emptyDataFrame());
    }

    @Override
    public boolean isValid() {
        return StringUtil.hasMoreThanOne(inputInfo.getSource(), inputInfo.getCrs(), inputInfo.getUsername())
                && StringUtil.hasMoreThanOne(inputInfo.getLayerNames()) && StringUtil.allNotNull(inputInfo.getPassword(),
                inputInfo.getGeometryFieldFormat(), inputInfo.getGeometryFieldNames(),
                inputInfo.getGeometryType(), inputInfo.getSerialField());
    }

    @Getter
    public abstract class RelationalDatabaseReaderHelper extends ReaderHelper {
        protected String[] tables;
        protected String username;
        protected String password;
        private final String driver;
        private final List<String> geometryFieldNameList;
        private final int numPartitions;
        private final int fetchSize = 2000;
        private final boolean continueBatchOnError = true;
        private final boolean pushDownPredicate = true;

        protected RelationalDatabaseReaderHelper(String source, String driver) {
            super(source);
            this.tables = inputInfo.getLayerNames();
            this.username = inputInfo.getUsername();
            this.password = inputInfo.getPassword();
            this.driver = driver;
            this.geometryFieldNameList = Arrays.asList(inputInfo.getGeometryFieldNames());
            this.numPartitions = ss.sparkContext().defaultParallelism();
        }

        @Override
        protected void initCharset() {
            super.charset = inputInfo.getCharset();
        }

        @Override
        protected void initReader() { }

        @Override
        protected void initFields() {
            this.initFieldsByDataset();
        }

        @Override
        protected void initCrs() {
            super.crs = CrsUtil.getByCode(inputInfo.getCrs());
        }

        @Override
        protected void initGeometryType() {
            // todo: verify the geometry type of database.
            super.geometryType = inputInfo.getGeometryType();
        }

        @Deprecated
        private void initFieldsByBasicConnection() {
            String sqlFrame = "select * from %s limit 1;";
            Connection connection;
            PreparedStatement statement;
            try {
                connection = DriverManager.getConnection(source, username, password);
                String sql = String.format(sqlFrame, tables[0]);
                statement = connection.prepareStatement(sql);
                ResultSetMetaData metaData = statement.getMetaData();
                int size = metaData.getColumnCount();
                super.fields = new Field[size - geometryFieldNameList.size()];
                for (int i = 0, j = 0; i < size; i++) {
                    String name = metaData.getColumnName(i + 1);
                    if (!geometryFieldNameList.contains(name)) {
                        super.fields[j++] = new Field(name, Class.forName(metaData.getColumnClassName(i + 1)));
                    }
                }
                statement.close();
                connection.close();
            } catch (SQLException | ClassNotFoundException e) {
                String msg = "Fail to init the mata of the database!";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        private void initFieldsByDataset() {
            Dataset<Row> df = readSmallTable(this, source, tables[0]);
            StructType schema = df.schema();
            super.fields = new Field[schema.length() - geometryFieldNameList.size()];
            for (int i = 0, j = 0; i < schema.length(); i++) {
                StructField field = schema.fields()[i];
                if (!geometryFieldNameList.contains(field.name())) {
                    super.fields[j++] = new Field(field.name(), field.dataType());
                }
            }
        }
    }
}
