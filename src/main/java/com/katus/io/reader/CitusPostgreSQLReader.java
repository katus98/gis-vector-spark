package com.katus.io.reader;

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
 * @author Sun Katus
 * @version 1.0, 2020-12-19
 * @since 1.2
 */
public class CitusPostgreSQLReader extends PostgreSQLReader {
    private final Map<String, List<Tuple2<String, String>>> sharedMaps;

    public CitusPostgreSQLReader(String url, String[] tables, String username, String password, String[] geometryFields, String crs, Boolean isWkt, String geometryType) {
        super(url, tables, username, password, "", geometryFields, crs, isWkt, geometryType);
        this.sharedMaps = new HashMap<>();
        initCitus();
    }

    private void initCitus() {
        Connection connection;
        Statement statement;
        try {
            connection = DriverManager.getConnection(url, username, password);
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
                    String l_url = String.format("jdbc:postgresql://%s:%d/%s", node, port, url.substring(url.lastIndexOf("/")+1));
                    String l_table = String.format("public.%s_%s", table, resultSet.getString("shardid"));
                    sharedTable.add(new Tuple2<>(l_url, l_table));
                }
                this.sharedMaps.put(table, sharedTable);
                resultSet.close();
            }
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Dataset<Row> readSingleTable(SparkSession ss, String table) {
        Dataset<Row> df = ss.emptyDataFrame();
        List<Tuple2<String, String>> tuple2List = sharedMaps.get(table);
        for (Tuple2<String, String> tuple2 : tuple2List) {
            df.union(readTable(ss, tuple2._2(), tuple2._1()));
        }
        return df;
    }
}
