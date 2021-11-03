package com.katus.io.reader;

import com.katus.entity.data.Layer;
import com.katus.entity.data.Table;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-14
 * @since 2.0
 */
@Slf4j
public class CitusPostgreSQLReader extends PostgreSQLReader {

    protected CitusPostgreSQLReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        CitusPostgreSQLReaderHelper readerHelper = this.new CitusPostgreSQLReaderHelper(inputInfo.getSource());
        return super.readToLayer(readerHelper, readMultipleTables(readerHelper));
    }

    @Override
    public Table readToTable() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        CitusPostgreSQLReaderHelper readerHelper = this.new CitusPostgreSQLReaderHelper(inputInfo.getSource());
        return new Table(readMultipleTables(readerHelper), readerHelper.fields);
    }

    @Override
    protected Dataset<Row> readSingleTable(RelationalDatabaseReaderHelper readerHelper, String table) {
        CitusPostgreSQLReaderHelper citusPostgreSQLReaderHelper = (CitusPostgreSQLReaderHelper) readerHelper;
        Dataset<Row> df = ss.emptyDataFrame();
        List<Tuple2<String, String>> tuple2List = citusPostgreSQLReaderHelper.sharedMaps.get(table);
        for (Tuple2<String, String> tuple2 : tuple2List) {
            df.union(readSmallTable(readerHelper, tuple2._1(), tuple2._2()));
        }
        return df;
    }

    @Getter
    public class CitusPostgreSQLReaderHelper extends PostgreSQLReaderHelper {
        private transient final Map<String, List<Tuple2<String, String>>> sharedMaps;

        protected CitusPostgreSQLReaderHelper(String source) {
            super(source);
            this.sharedMaps = new HashMap<>();
            this.initCitus();
        }

        private void initCitus() {
            Connection connection;
            Statement statement;
            try {
                connection = DriverManager.getConnection(source, username, password);
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                for (String table : tables) {
                    String sql = String.format("select * from pg_dist_shard_placement where shardid " +
                            "in (select shardid from pg_dist_shard where logicalrelid='public.%s' ::regclass)", table);
                    ResultSet resultSet = statement.executeQuery(sql);
                    List<Tuple2<String, String>> sharedTable = new ArrayList<>();
                    while (resultSet.next()) {
                        String node = resultSet.getString("nodename");
                        Integer port = resultSet.getInt("nodeport");
                        String l_source = String.format("jdbc:postgresql://%s:%d/%s", node, port, source.substring(source.lastIndexOf("/") + 1));
                        String l_table = String.format("public.%s_%s", table, resultSet.getString("shardid"));
                        sharedTable.add(new Tuple2<>(l_source, l_table));
                    }
                    this.sharedMaps.put(table, sharedTable);
                    resultSet.close();
                }
                statement.close();
                connection.close();
            } catch (SQLException e) {
                String msg = "Fail to init Citus information!";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }
    }
}
