package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
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
 * @author WANG Mengxiao, SUN Katus
 * @version 1.2, 2020-12-12
 */
@Slf4j
public class Union {

    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        UnionArgs mArgs = new UnionArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Union Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader1 = ReaderFactory.create(ss, mArgs.getInput1());
        Reader reader2 = ReaderFactory.create(ss, mArgs.getInput2());
        Layer inputLayer = reader1.readToLayer();
        Layer overlayLayer = reader2.readToLayer();

        log.info("Dimension check");
        if (inputLayer.getMetadata().getGeometryType().getDimension() != overlayLayer.getMetadata().getGeometryType().getDimension()) {
            String msg = "Two layers must have the same dimension, exit!";
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
        Layer layer = union(inputLayer, overlayLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

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
        int dimension = metadata1.getGeometryType().getDimension();
        Field[] fields = FieldUtil.merge(metadata1.getFields(), metadata2.getFields());
        JavaPairRDD<String, Feature> result = indexedLayer1.fullOuterJoin(indexedLayer2)
                .flatMapToPair(fullPairItems -> {
                    List<Tuple2<String, Feature>> resultList = new ArrayList<>();
                    Feature f1 = null, f2 = null;
                    Feature feature1 = null, feature2 = null;
                    if (fullPairItems._2()._1().isPresent()) feature1 = fullPairItems._2()._1().get();
                    if (fullPairItems._2()._2().isPresent()) feature2 = fullPairItems._2()._2().get();
                    if (feature1 != null && feature2 != null && feature1.getGeometry().intersects(feature2.getGeometry())) {
                        Geometry diff = GeometryUtil.breakByDimension(feature2.getGeometry().difference(feature1.getGeometry()), dimension);
                        LinkedHashMap<Field, Object> attributes = AttributeUtil.merge(fields, feature1.getAttributes(), feature2.getAttributes());
                        f1 = new Feature(feature1.getFid(), attributes, feature1.getGeometry());
                        if (!diff.isEmpty()) f2 = new Feature(feature2.getFid(), attributes, diff);
                    } else {
                        if (feature1 != null) {
                            LinkedHashMap<Field, Object> attributes1 = AttributeUtil.merge(fields, feature1.getAttributes(), new HashMap<>());
                            f1 = new Feature(feature1.getFid(), attributes1, feature1.getGeometry());
                        }
                        if (feature2 != null) {
                            LinkedHashMap<Field, Object> attributes2 = AttributeUtil.merge(fields, new HashMap<>(), feature2.getAttributes());
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
