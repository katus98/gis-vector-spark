package com.katus.model.gpt;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.dmt.Merge;
import com.katus.model.gpt.args.UnionArgs;
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
 * @author Wang Mengxiao (wmx), Sun Katus
 * @version 1.2, 2020-12-12
 */
@Slf4j
public class Union {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        UnionArgs mArgs = UnionArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Union Args failed, exit!";
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
        if (GeometryUtil.getDimensionOfGeomType(layer1.getMetadata().getGeometryType()) !=
                GeometryUtil.getDimensionOfGeomType(layer2.getMetadata().getGeometryType())) {
            String msg = "Two layers must have the same dimension, exit!";
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
        Layer layer = union(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer union(Layer layer1, Layer layer2) {
        Layer in = Intersection.intersection(layer1, layer2);
        Layer out = SymmetricalDifference.symmetricalDifference(layer1, layer2);
        return Merge.merge(in, out);
    }

    @Deprecated
    public static Layer unionWithClippedIndex(Layer indexedLayer1, Layer indexedLayer2) {
        LayerMetadata metadata1 = indexedLayer1.getMetadata();
        LayerMetadata metadata2 = indexedLayer2.getMetadata();
        int dimension = GeometryUtil.getDimensionOfGeomType(metadata1.getGeometryType());
        String[] fields = FieldUtil.merge(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result = indexedLayer1.fullOuterJoin(indexedLayer2)
                .flatMapToPair(fullPairItems -> {
                    List<Tuple2<String, Feature>> resultList = new ArrayList<>();
                    Feature f1 = null, f2 = null;
                    Feature feature1 = null, feature2 = null;
                    if (fullPairItems._2()._1().isPresent()) feature1 = fullPairItems._2()._1().get();
                    if (fullPairItems._2()._2().isPresent()) feature2 = fullPairItems._2()._2().get();
                    if (feature1 != null && feature2 != null && feature1.getGeometry().intersects(feature2.getGeometry())) {
                        Geometry diff = GeometryUtil.breakByDimension(feature2.getGeometry().difference(feature1.getGeometry()), dimension);
                        LinkedHashMap<String, Object> attributes = AttributeUtil.merge(fields, feature1.getAttributes(), feature2.getAttributes());
                        f1 = new Feature(feature1.getFid(), attributes, feature1.getGeometry());
                        if (!diff.isEmpty()) f2 = new Feature(feature2.getFid(), attributes, diff);
                    } else {
                        if (feature1 != null) {
                            LinkedHashMap<String, Object> attributes1 = AttributeUtil.merge(fields, feature1.getAttributes(), new HashMap<>());
                            f1 = new Feature(feature1.getFid(), attributes1, feature1.getGeometry());
                        }
                        if (feature2 != null) {
                            LinkedHashMap<String, Object> attributes2 = AttributeUtil.merge(fields, new HashMap<>(), feature2.getAttributes());
                            f2 = new Feature(feature2.getFid(), attributes2, feature2.getGeometry());
                        }
                    }
                    if (f1 != null) resultList.add(new Tuple2<>(fullPairItems._1() + "#" + f1.getFid(), f1));
                    if (f2 != null) resultList.add(new Tuple2<>(fullPairItems._1() + "#" + f2.getFid(), f2));
                    return resultList.iterator();
                })
                .reduceByKey((feature1, feature2) -> {
                    if (feature1.getGeometry().equals(feature2.getGeometry())) return feature1;
                    if (feature1.getGeometry().intersects(feature2.getGeometry())) {
                        Geometry geometry = GeometryUtil.breakByDimension(feature1.getGeometry().intersection(feature2.getGeometry()), dimension);
                        return new Feature(feature1.getFid(), feature1.getAttributes(), geometry);
                    } else return Feature.EMPTY_FEATURE;
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .cache();
        return Layer.create(result, fields, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}
