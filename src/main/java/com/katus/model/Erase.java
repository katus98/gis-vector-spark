package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.EraseArgs;
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
 * @version 1.1, 2020-12-08
 */
@Slf4j
public class Erase {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        EraseArgs mArgs = EraseArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Erase Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1(), mArgs.getSerialField1());
        Layer extentLayer = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
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
        targetLayer = targetLayer.index(14);
        extentLayer = extentLayer.index(14);

        log.info("Start Calculation");
        Layer layer = erase(targetLayer, extentLayer);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }

    public static Layer erase(Layer tarIndLayer, Layer extIndLayer) {
        LayerMetadata metadata = tarIndLayer.getMetadata();
        int dimension = GeometryUtil.getDimensionOfGeomType(metadata.getGeometryType());
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
                    return new Tuple2<>(leftPairItems._1() + "#" + feature.getFid(), feature);
                })
                .mapToPair(pairItem -> {
                    Feature feature = pairItem._2();
                    feature.setGeometry(GeometryUtil.breakGeometryCollectionByDimension(feature.getGeometry(), dimension));
                    return new Tuple2<>(pairItem._1(), feature);
                })
                .filter(pairItem -> pairItem._2().hasGeometry() && GeometryUtil.getDimensionOfGeomType(pairItem._2().getGeometry()) == dimension)
                .cache();
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}
