package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.BufferArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author WANG Mengxiao, SUN Katus
 * @version 1.2, 2020-12-14
 */
@Slf4j
public class Buffer {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        BufferArgs mArgs = new BufferArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Buffer Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput());

        log.info("Prepare calculation");
        CoordinateReferenceSystem oriCrs = inputLayer.getMetadata().getCrs();
        if (!mArgs.getCrs().isEmpty()) {
            CoordinateReferenceSystem crsU = CrsUtil.getByCode(mArgs.getCrs());
            if (!oriCrs.equals(crsU)) {
                inputLayer = inputLayer.project(crsU);
            }
        }

        log.info("Start Calculation");
        Layer layer = buffer(inputLayer, Double.parseDouble(mArgs.getDistance()));

        log.info("Post calculation");
        if (!layer.getMetadata().getCrs().equals(oriCrs)) {
            layer = layer.project(oriCrs);
        }

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer buffer(Layer layer, Double distance) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.mapToPair(pairItem -> {
            pairItem._2().setGeometry(pairItem._2().getGeometry().buffer(distance));
            return pairItem;
        }).cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), "Polygon", metadata.getFeatureCount());
    }
}
