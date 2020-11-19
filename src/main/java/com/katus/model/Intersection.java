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
 * @author Keran Sun (katus)
 * @version 2.0, 2020-11-19
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
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2());

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            layer1 = layer1.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            layer2 = layer2.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        layer1 = layer1.index(14);
        layer2 = layer2.index(14);

        log.info("Start Calculation");
        Layer layer = intersection(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer intersection(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata1 = tarIndLayer.getMetadata();
        LayerMetadata metadata2 = extIndLayer.getMetadata();
        int dimension1 = GeometryUtil.getDimensionOfGeomType(metadata1.getGeometryType());
        int dimension2 = GeometryUtil.getDimensionOfGeomType(metadata2.getGeometryType());
        int dimension = Math.min(dimension1, dimension2);
        String[] fieldNames = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result = tarIndLayer.join(extIndLayer)
                .mapToPair(pairItems -> {
                    Feature targetFeature = pairItems._2()._1();
                    Feature extentFeature = pairItems._2()._2();
                    Geometry geoTarget = targetFeature.getGeometry();
                    Geometry geoExtent = extentFeature.getGeometry();
                    Feature feature = null;
                    if (geoTarget.intersects(geoExtent)) {
                        String fid = targetFeature.getFid() + "#" + extentFeature.getFid();
                        LinkedHashMap<String, Object> attributes = AttributeUtil.merge(fieldNames, targetFeature.getAttributes(), extentFeature.getAttributes());
                        Geometry inter = geoExtent.intersection(geoTarget);
                        inter = GeometryUtil.breakGeometryCollectionByDimension(inter, dimension);
                        feature = new Feature(fid, attributes, inter);
                    }
                    return new Tuple2<>(pairItems._1(), feature);
                })
                .filter(pairItem -> pairItem._2() != null && GeometryUtil.getDimensionOfGeomType(pairItem._2().getGeometry()) == dimension)
                .cache();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
