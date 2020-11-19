package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.SymmetricalDifferenceArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 2.0, 2020-11-19
 */
@Slf4j
public class SymmetricalDifference {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        SymmetricalDifferenceArgs mArgs = SymmetricalDifferenceArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Symmetrical Difference Args failed, exit!";
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

        log.info("Dimension check");
        if (GeometryUtil.getDimensionOfGeomType(layer1.getMetadata().getGeometryType()) != 2 ||
                GeometryUtil.getDimensionOfGeomType(layer2.getMetadata().getGeometryType()) != 2) {
            String msg = "Geometry dimension must be 2, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

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
        Layer layer = symmetricalDifference(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer symmetricalDifference(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        String[] fieldNames = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result = layer1.fullOuterJoin(layer2)
                .flatMapToPair(fullPairItems -> {
                    List<Tuple2<String, Feature>> resultList = new ArrayList<>();
                    Feature tarFeature = fullPairItems._2()._1().isPresent() ? fullPairItems._2()._1().get() : null;
                    Feature extFeature = fullPairItems._2()._2().isPresent() ? fullPairItems._2()._2().get() : null;
                    LinkedHashMap<String, Object> attributes1, attributes2;
                    Feature feature1 = null, feature2 = null;
                    if (tarFeature != null && extFeature != null) {
                        attributes1 = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), new HashMap<>());
                        attributes2 = AttributeUtil.merge(fieldNames, new HashMap<>(), extFeature.getAttributes());
                        feature1 = new Feature(tarFeature.getFid(), attributes1, tarFeature.getGeometry().difference(extFeature.getGeometry()));
                        feature2 = new Feature(extFeature.getFid(), attributes2, extFeature.getGeometry().difference(tarFeature.getGeometry()));
                    } else {
                        if (tarFeature != null) {
                            attributes1 = AttributeUtil.merge(fieldNames, tarFeature.getAttributes(), new HashMap<>());
                            feature1 = new Feature(tarFeature.getFid(), attributes1, tarFeature.getGeometry());
                        }
                        if (extFeature != null) {
                            attributes2 = AttributeUtil.merge(fieldNames, new HashMap<>(), extFeature.getAttributes());
                            feature2 = new Feature(extFeature.getFid(), attributes2, extFeature.getGeometry());
                        }
                    }
                    if (feature1 != null) resultList.add(new Tuple2<>(fullPairItems._1() + "#" + feature1.getFid(), feature1));
                    if (feature2 != null) resultList.add(new Tuple2<>(fullPairItems._1() + "#" + feature2.getFid(), feature2));
                    return resultList.iterator();
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .reduceByKey((feature1, feature2) -> {
                    if (feature1 == null || feature2 == null) return null;
                    feature1.setGeometry(GeometryUtil.breakGeometryCollectionByDimension(feature1.getGeometry(), 2));
                    feature2.setGeometry(GeometryUtil.breakGeometryCollectionByDimension(feature2.getGeometry(), 2));
                    if (GeometryUtil.getDimensionOfGeomType(feature1.getGeometry()) != 2) return null;
                    if (GeometryUtil.getDimensionOfGeomType(feature2.getGeometry()) != 2) return null;
                    if (feature1.getGeometry().intersects(feature2.getGeometry())) {
                        Geometry inter = feature1.getGeometry().intersection(feature2.getGeometry());
                        return new Feature(feature1.getFid(), feature1.getAttributes(), inter);
                    } else {
                        return null;
                    }
                })
                .filter(pairItem -> pairItem._2() != null)
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    feature.setGeometry(GeometryUtil.breakGeometryCollectionByDimension(feature.getGeometry(), 2));
                    return new Tuple2<>(pairItem._1(), feature);
                })
                .filter(pairItem -> pairItem._2().hasGeometry() && GeometryUtil.getDimensionOfGeomType(pairItem._2().getGeometry()) == 2)
                .cache();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
