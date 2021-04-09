package com.katus.model.rt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.rt.args.RandomSelectionArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class RandomSelection {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        RandomSelectionArgs mArgs = new RandomSelectionArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Random Selection Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput());

        log.info("Prepare calculation");
        String rs = mArgs.getRatio().trim();
        double ratio = rs.endsWith("%") ? Double.parseDouble(rs.substring(0, rs.length()-1)) / 100.0 : Double.parseDouble(rs);
        if (ratio > 1 || ratio < 0) {
            String msg = "Ratio must between 0 and 1, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Start Calculation");
        Layer layer = randomSelection(inputLayer, ratio);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer randomSelection(Layer layer, double fraction) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.sample(false, fraction).cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
