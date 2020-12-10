package com.katus;

import com.katus.entity.Layer;
import com.katus.io.lg.RelationalDatabaseLayerGenerator;
import com.katus.io.reader.PostgreSQLReader;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.util.SparkUtil;
import org.apache.spark.sql.SparkSession;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-07
 * @since 1.1
 */
public class DbTest {
    public static void main(String[] args) throws Exception {
        SparkSession ss = SparkUtil.getSparkSession();
        testPG(ss);
        ss.close();
    }

    public static void testPG(SparkSession ss) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/geo_db";
        String[] tables = new String[]{"public.test"};
        String username = "postgres", password = "postgres", idField = "id";
        PostgreSQLReader reader = new PostgreSQLReader(url, tables, username, password, idField, new String[]{"wkt"}, "4326");
        RelationalDatabaseLayerGenerator lg = new RelationalDatabaseLayerGenerator(ss, reader);
        Layer layer = lg.generate();
        Layer layer1 = Layer.create(layer.filter(pairItem -> (Integer) pairItem._2().getAttribute("id") <= 10000), layer.getMetadata());
        LayerTextFileWriter writer = new LayerTextFileWriter("", "/D:/Data/gjj/test.csv");
        writer.writeToFileByPartCollect(layer1, true, false, true);
    }
}
