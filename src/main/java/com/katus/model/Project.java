package com.katus.model;

import com.katus.entity.Layer;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.ProjectArgs;
import com.katus.util.CrsUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-08
 * @since 1.1
 */
@Slf4j
public class Project {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        ProjectArgs mArgs = ProjectArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Project Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType(), mArgs.getSerialField());

        log.info("Start Calculation");
        CoordinateReferenceSystem tarCrs = CrsUtil.getByCode(mArgs.getTargetCrs());
        Layer layer = targetLayer.getMetadata().getCrs().equals(tarCrs) ? targetLayer : targetLayer.project(tarCrs);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getNeedHeader()), false, true);

        ss.close();
    }
}
