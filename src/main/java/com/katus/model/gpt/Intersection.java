package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.IntersectionArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.LinkedHashMap;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-11
 */
@Slf4j
public class Intersection {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        IntersectionArgs mArgs = new IntersectionArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Intersection Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer overlayLayer = InputUtil.makeLayer(ss, mArgs.getInput2());

        log.info("Prepare calculation");
        if (!mArgs.getInput1().getCrs().equals(mArgs.getInput2().getCrs())) {
            overlayLayer = overlayLayer.project(CrsUtil.getByCode(mArgs.getInput1().getCrs()));
        }
        inputLayer = inputLayer.index();
        overlayLayer = overlayLayer.index();

        log.info("Start Calculation");
        Layer layer = intersection(inputLayer, overlayLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer intersection(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        int dimension1 = GeometryUtil.getDimensionOfGeomType(metadata1.getGeometryType());
        int dimension2 = GeometryUtil.getDimensionOfGeomType(metadata2.getGeometryType());
        int dimension = Math.min(dimension1, dimension2);
        Field[] fields = FieldUtil.merge(metadata1.getFields(), metadata2.getFields());
        JavaPairRDD<String, Feature> result = layer1.join(layer2)
                .mapToPair(pairItems -> {
                    Feature targetFeature = pairItems._2()._1();
                    Feature extentFeature = pairItems._2()._2();
                    Geometry geoTarget = targetFeature.getGeometry();
                    Geometry geoExtent = extentFeature.getGeometry();
                    Feature feature;
                    String key = "";
                    if (geoTarget.intersects(geoExtent)) {
                        key = targetFeature.getFid() + "#" + extentFeature.getFid();
                        LinkedHashMap<Field, Object> attributes = AttributeUtil.merge(fields, targetFeature.getAttributes(), extentFeature.getAttributes());
                        Geometry inter = geoExtent.intersection(geoTarget);
                        inter = GeometryUtil.breakByDimension(inter, dimension);
                        feature = new Feature(targetFeature.getFid(), attributes, inter);
                    } else {
                        feature = Feature.EMPTY_FEATURE;
                    }
                    return new Tuple2<>(key, feature);
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .reduceByKey((f1, f2) -> f1)
                .cache();
        return Layer.create(result, fields, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
