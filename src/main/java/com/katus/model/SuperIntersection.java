package com.katus.model;

import com.katus.constant.ProvinceExtent;
import com.katus.entity.Layer;
import com.katus.entity.Pyramid;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.IntersectionArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-11
 * @since 1.2
 */
@Slf4j
public class SuperIntersection {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        IntersectionArgs mArgs = IntersectionArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Super Intersection Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        String input1 = mArgs.getInput1();
        String input2 = mArgs.getInput2();
        if (input1.startsWith("citus:") && input2.startsWith("citus:")) {
            input1 = input1.substring(6);
            input2 = input2.substring(6);
        } else {
            String msg = "Super Intersection Input not correct, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        String[] tables1 = input1.split(",");
        String[] tables2 = input2.split(",");
        for (int i = 0; i < tables1.length && i < tables2.length; i++) {
            int z = 1;
            Pyramid pyramid = new Pyramid(ProvinceExtent.getExtentByName(tables1[i]), z);
            int n = 1 << z;
            for (int x = 0; x < n; x++) {
                for (int y = 0; y < n; y++) {
                    Layer layer1 = InputUtil.makeLayer(ss, tables1[i], pyramid.getTile(x, y),
                            mArgs.getGeometryFields1().split(","), mArgs.getCrs1());
                    Layer layer2 = InputUtil.makeLayer(ss, tables2[i], pyramid.getTile(x, y),
                            mArgs.getGeometryFields2().split(","), mArgs.getCrs2());
                    log.info("Prepare calculation-" + i + "-" + x + "-" + y);
                    if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
                        layer1 = layer1.project(CrsUtil.getByCode(mArgs.getCrs()));
                    }
                    if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
                        layer2 = layer2.project(CrsUtil.getByCode(mArgs.getCrs()));
                    }
                    layer1 = layer1.index();
                    layer2 = layer2.index();

                    log.info("Start Calculation-" + i + "-" + x + "-" + y);
                    Layer layer = Intersection.intersection(layer1, layer2);

                    log.info("Output result-" + i + "-" + x + "-" + y);
                    LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput() + i + "-" + x + "-" + y);
                    writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);
                }
            }
        }
        ss.close();
    }
}
