package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.MultiToSingleArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 2.0, 2020-11-19
 */
@Slf4j
public class MultiToSingle {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        MultiToSingleArgs mArgs = MultiToSingleArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Multi To Single Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType());

        log.info("Start Calculation");
        Layer layer = multiToSingle(targetLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer multiToSingle(Layer layer) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer
                .flatMapToPair(pairItem -> { 
                    List<Tuple2<String, Feature>> resultList = new ArrayList<>();
                    Feature feature = pairItem._2();
                    Geometry geometry = feature.getGeometry();
                    for (int i = 0; i < geometry.getNumGeometries(); i++) { 
                        resultList.add(new Tuple2<>(pairItem._1(), new Feature(feature.getFid(), feature.getAttributes(), geometry.getGeometryN(i)))); 
                    }
                    return resultList.iterator(); 
                })
                .cache();
        String geometryType = metadata.getGeometryType().replace("Multi", "");
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), geometryType, result.count());
    }
}
