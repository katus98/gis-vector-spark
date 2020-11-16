package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.ClipArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-16
 */
@Slf4j
public class Clip {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ClipArgs mArgs = ClipArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Clip Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1());
        Layer extentLayer = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2());

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            targetLayer = targetLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            extentLayer = extentLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        targetLayer = targetLayer.index(14);
        extentLayer = extentLayer.index(14);

        log.info("Start Calculation");
        Layer layer = clip(targetLayer, extentLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);

        ss.close();
    }

    public static Layer clip(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata = tarIndLayer.getMetadata();
        JavaPairRDD<String, Feature> result = tarIndLayer.join(extIndLayer)
                .mapToPair(pairItems -> {
                    Feature targetFeature = pairItems._2()._1();
                    Feature extentFeature = pairItems._2()._2();
                    Geometry geoTarget = targetFeature.getGeometry();
                    Geometry geoExtent = extentFeature.getGeometry();
                    Feature feature = new Feature();
                    feature.setFid("EMPTY");
                    if (geoTarget.intersects(geoExtent)) {
                        Geometry inter = geoExtent.intersection(geoTarget);
                        feature.setFid(targetFeature.getFid());
                        feature.setAttributes(targetFeature.getAttributes());
                        feature.setGeometry(inter);
                    }
                    return new Tuple2<>(pairItems._1(), feature);
                })
                .filter(pairItem -> !pairItem._2().getFid().equals("EMPTY"))
                .cache();
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
