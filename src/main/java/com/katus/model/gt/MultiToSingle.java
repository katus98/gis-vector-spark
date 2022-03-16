package com.katus.model.gt;

import com.katus.constant.GeometryType;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gt.args.MultiToSingleArgs;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class MultiToSingle {

    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        MultiToSingleArgs mArgs = new MultiToSingleArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Multi To Single Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader = ReaderFactory.create(ss, mArgs.getInput());
        Layer inputLayer = reader.readToLayer();

        log.info("Start Calculation");
        Layer layer = multiToSingle(inputLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

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
        GeometryType geometryType = metadata.getGeometryType().getBasicByDimension();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), geometryType, result.count());
    }
}
