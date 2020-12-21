package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.IntersectionArgs;
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
        IntersectionArgs mArgs = IntersectionArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Intersection Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer layer1 = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1(), mArgs.getSerialField1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2(), mArgs.getSerialField2());

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            layer1 = layer1.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            layer2 = layer2.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        layer1 = layer1.index();
        layer2 = layer2.index();

        log.info("Start Calculation");
        Layer layer = intersection(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer intersection(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        int dimension1 = GeometryUtil.getDimensionOfGeomType(metadata1.getGeometryType());
        int dimension2 = GeometryUtil.getDimensionOfGeomType(metadata2.getGeometryType());
        int dimension = Math.min(dimension1, dimension2);
        String[] fieldNames = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
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
                        LinkedHashMap<String, Object> attributes = AttributeUtil.merge(fieldNames, targetFeature.getAttributes(), extentFeature.getAttributes());
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
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
