package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.BufferArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Mengxiao Wang (wmx), Sun Katus
 * @version 1.2, 2020-12-14
 */
@Slf4j
public class Buffer {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        BufferArgs mArgs = BufferArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Buffer Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Prepare calculation");
        CoordinateReferenceSystem oriCrs = targetLayer.getMetadata().getCrs();
        if (!mArgs.getCrsUnit().isEmpty()) {
            CoordinateReferenceSystem crsU = CrsUtil.getByCode(mArgs.getCrsUnit());
            if (!oriCrs.equals(crsU)) {
                targetLayer = targetLayer.project(crsU);
            }
        }

        log.info("Start Calculation");
        Layer layer = buffer(targetLayer, Double.parseDouble(mArgs.getDistance()));

        log.info("Post calculation");
        if (!layer.getMetadata().getCrs().equals(oriCrs)) {
            layer = layer.project(oriCrs);
        }

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer buffer(Layer layer, Double distance) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.mapToPair(pairItem -> {
            pairItem._2().setGeometry(pairItem._2().getGeometry().buffer(distance));
            return pairItem;
        }).cache();
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), "Polygon", metadata.getFeatureCount());
    }
}
