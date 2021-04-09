package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
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
        SymmetricalDifferenceArgs mArgs = new SymmetricalDifferenceArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Symmetrical Difference Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer inputLayer = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer overlayLayer = InputUtil.makeLayer(ss, mArgs.getInput2());

        log.info("Dimension check");
        if (GeometryUtil.getDimensionOfGeomType(inputLayer.getMetadata().getGeometryType()) != 2 ||
                GeometryUtil.getDimensionOfGeomType(overlayLayer.getMetadata().getGeometryType()) != 2) {
            String msg = "Geometry dimension must be 2, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Prepare calculation");
        if (!mArgs.getInput1().getCrs().equals(mArgs.getInput2().getCrs())) {
            overlayLayer = overlayLayer.project(CrsUtil.getByCode(mArgs.getInput1().getCrs()));
        }
        inputLayer = inputLayer.index();
        overlayLayer = overlayLayer.index();

        log.info("Start Calculation");
        Layer layer = symmetricalDifference(inputLayer, overlayLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer symmetricalDifference(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        Field[] fields = FieldUtil.merge(metadata1.getFields(), metadata2.getFields());
        JavaPairRDD<String, Feature> result = layer1.fullOuterJoin(layer2)
                .flatMapToPair(fullPairItems -> {
                    List<Tuple2<String, Feature>> resultList = new ArrayList<>();
                    Feature tarFeature = fullPairItems._2()._1().isPresent() ? fullPairItems._2()._1().get() : null;
                    Feature extFeature = fullPairItems._2()._2().isPresent() ? fullPairItems._2()._2().get() : null;
                    LinkedHashMap<Field, Object> attributes1, attributes2;
                    Feature feature1 = null, feature2 = null;
                    if (tarFeature != null && extFeature != null) {
                        attributes1 = AttributeUtil.merge(fields, tarFeature.getAttributes(), new HashMap<>());
                        attributes2 = AttributeUtil.merge(fields, new HashMap<>(), extFeature.getAttributes());
                        feature1 = new Feature(tarFeature.getFid(), attributes1, tarFeature.getGeometry().difference(extFeature.getGeometry()));
                        feature2 = new Feature(extFeature.getFid(), attributes2, extFeature.getGeometry().difference(tarFeature.getGeometry()));
                    } else {
                        if (tarFeature != null) {
                            attributes1 = AttributeUtil.merge(fields, tarFeature.getAttributes(), new HashMap<>());
                            feature1 = new Feature(tarFeature.getFid(), attributes1, tarFeature.getGeometry());
                        }
                        if (extFeature != null) {
                            attributes2 = AttributeUtil.merge(fields, new HashMap<>(), extFeature.getAttributes());
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
        return Layer.create(result, fields, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
