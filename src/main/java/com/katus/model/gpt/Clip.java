package com.katus.model.gpt;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.ClipArgs;
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
public class Clip {

    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ClipArgs mArgs = new ClipArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Clip Args are not valid, exit!";
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
        Layer layer = clip(inputLayer, overlayLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }

    public static Layer clip(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata = tarIndLayer.getMetadata();
        int dimension = metadata.getGeometryType().getDimension();
        JavaPairRDD<String, Feature> result = tarIndLayer.join(extIndLayer)
                .mapToPair(pairItems -> {
                    Feature targetFeature = pairItems._2()._1();
                    Feature extentFeature = pairItems._2()._2();
                    Geometry geoTarget = targetFeature.getGeometry();
                    Geometry geoExtent = extentFeature.getGeometry();
                    Feature feature;
                    String key = "";
                    if (geoTarget.intersects(geoExtent)) {
                        key = targetFeature.getFid() + "#" + extentFeature.getFid();
                        Geometry inter = geoExtent.intersection(geoTarget);
                        inter = GeometryUtil.breakByDimension(inter, dimension);
                        feature = new Feature(targetFeature.getFid(), targetFeature.getAttributes(), inter);
                    } else {
                        feature = Feature.EMPTY_FEATURE;
                    }
                    return new Tuple2<>(key, feature);
                })
                .filter(pairItem -> pairItem._2().hasGeometry())
                .reduceByKey((f1, f2) -> f1)
                .mapToPair(pairItem -> new Tuple2<>(pairItem._2().getFid(), pairItem._2()))
                .reduceByKey((f1, f2) -> {
                    f1.setGeometry(f1.getGeometry().union(f2.getGeometry()));
                    return f1;
                })
                .cache();
        return Layer.create(result, metadata.getFields(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
