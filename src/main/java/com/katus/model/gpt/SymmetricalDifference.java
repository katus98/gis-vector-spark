package com.katus.model.gpt;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.SymmetricalDifferenceArgs;
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
 * @author Sun Katus
 * @version 1.2, 2020-12-12
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
        Layer layer1 = InputUtil.makeLayer(ss, mArgs.getInput1(), mArgs.getLayers1().split(","), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1(), mArgs.getSerialField1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2(), mArgs.getLayers2().split(","), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2(), mArgs.getSerialField2());

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
        layer1 = layer1.index();
        layer2 = layer2.index();

        log.info("Start Calculation");
        Layer layer = symmetricalDifference(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
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
                    if (feature1 != null) resultList.add(new Tuple2<>(feature1.getFid(), feature1));
                    if (feature2 != null) resultList.add(new Tuple2<>(feature2.getFid(), feature2));
                    return resultList.iterator();
                })
                .reduceByKey((feature1, feature2) -> {
                    if (feature1.getGeometry().intersects(feature2.getGeometry())) {
                        Geometry inter = feature1.getGeometry().intersection(feature2.getGeometry());
                        return new Feature(feature1.getFid(), feature1.getAttributes(), GeometryUtil.breakByDimension(inter, 2));
                    } else {
                        return Feature.EMPTY_FEATURE;
                    }
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .cache();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}