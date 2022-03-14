package com.katus.model.dmt;

import com.katus.entity.data.Layer;
import com.katus.io.reader.Reader;
import com.katus.io.reader.ReaderFactory;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.dmt.args.ProjectArgs;
import com.katus.util.CrsUtil;
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
        ProjectArgs mArgs = new ProjectArgs(args);
        if (!mArgs.isValid()) {
            String msg = "Project Args are not valid, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Reader reader = ReaderFactory.create(ss, mArgs.getInput());
        Layer targetLayer = reader.readToLayer();

        log.info("Start Calculation");
        CoordinateReferenceSystem tarCrs = CrsUtil.getByCode(mArgs.getCrs());
        Layer layer = targetLayer.getMetadata().getCrs().equals(tarCrs) ? targetLayer : targetLayer.project(tarCrs);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter(mArgs.getOutput().getDestination());
        writer.writeToFileByPartCollect(layer, Boolean.parseBoolean(mArgs.getOutput().getHeader()), false, true);

        ss.close();
    }
}
