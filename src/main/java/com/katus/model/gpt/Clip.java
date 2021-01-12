package com.katus.model.gpt;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.gpt.args.ClipArgs;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import com.katus.util.InputUtil;
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
        ClipArgs mArgs = ClipArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Clip Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput1(), mArgs.getLayers1().split(","), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1(), mArgs.getSerialField1());
        Layer extentLayer = InputUtil.makeLayer(ss, mArgs.getInput2(), mArgs.getLayers2().split(","), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2(), mArgs.getSerialField2());

        log.info("Dimension check");
        if (GeometryUtil.getDimensionOfGeomType(extentLayer.getMetadata().getGeometryType()) != 2) {
            String msg = "Extent Geometry dimension must be 2, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            targetLayer = targetLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            extentLayer = extentLayer.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        targetLayer = targetLayer.index();
        extentLayer = extentLayer.index();

        log.info("Start Calculation");
        Layer layer = clip(targetLayer, extentLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer clip(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata = tarIndLayer.getMetadata();
        int dimension = GeometryUtil.getDimensionOfGeomType(metadata.getGeometryType());
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
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
