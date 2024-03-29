package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.EraseArgs;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-11
 */
@Slf4j
public class Erase {

    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        EraseArgs mArgs = new EraseArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Erase Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader1 = ReaderFactory.create(ss, mArgs.getInput1());
        Reader reader2 = ReaderFactory.create(ss, mArgs.getInput2());
        Layer inputLayer = reader1.readToLayer();
        Layer overlayLayer = reader2.readToLayer();

        log.info("Dimension check");
        if (overlayLayer.getMetadata().getGeometryType().getDimension() != 2) {
            String msg = "Extent Geometry dimension must be 2, exit!";
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
        Layer layer = erase(inputLayer, overlayLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer erase(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata = tarIndLayer.getMetadata();
        int dimension = metadata.getGeometryType().getDimension();
        JavaPairRDD<String, Feature> reducedExtRDD = extIndLayer.reduceByKey((feature1, feature2) -> {
            Geometry whole = feature1.getGeometry().union(feature2.getGeometry());
            return new Feature(whole);
        });
        JavaPairRDD<String, Feature> result = tarIndLayer.leftOuterJoin(reducedExtRDD)
                .mapToPair(leftPairItems -> {
                    Feature feature;
                    Feature tarFeature = leftPairItems._2()._1();
                    if (leftPairItems._2()._2().isPresent()) {
                        Feature extFeature = leftPairItems._2()._2().get();
                        if (tarFeature.getGeometry().intersects(extFeature.getGeometry())) {
                            Geometry outer = tarFeature.getGeometry().difference(extFeature.getGeometry());
                            feature = new Feature(tarFeature.getFid(), tarFeature.getAttributes(), outer);
                        } else {
                            feature = tarFeature;
                        }
                    } else {
                        feature = tarFeature;
                    }
                    return new Tuple2<>(feature.getFid(), feature);
                })
                .reduceByKey((f1, f2) -> {
                    Geometry geometry1 = f1.getGeometry();
                    Geometry geometry2 = f2.getGeometry();
                    if (geometry1.intersects(geometry2)) {
                        f1.setGeometry(geometry1.intersection(geometry2));
                        return f1;
                    } else {
                        return Feature.EMPTY_FEATURE;
                    }
                })
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    feature.setGeometry(GeometryUtil.breakByDimension(feature.getGeometry(), dimension));
                    return new Tuple2<>(pairItem._1(), feature);
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
