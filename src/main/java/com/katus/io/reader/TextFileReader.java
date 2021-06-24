package com.katus.io.reader;

import com.katus.entity.LayerMetadata;
import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.data.Table;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import com.katus.util.ArrayUtil;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import com.katus.util.StringUtil;
import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-13
 * @since 2.0
 */
@Slf4j
public class TextFileReader extends Reader {

    protected TextFileReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        LongAccumulator dataItemErrorCount = ss.sparkContext().longAccumulator("DataItemErrorCount");
        TextFileReaderHelper readerHelper = this.new TextFileReaderHelper(inputInfo.getSource());
        JavaRDD<String> lineRDD = jsc.textFile(readerHelper.source).repartition(jsc.defaultParallelism());
        Field[] fields = readerHelper.getFields();
        if (inputInfo.getHeader()) {
            lineRDD = lineRDD.filter(line -> !line.startsWith(fields[0].getName()));
        }
        JavaPairRDD<String, Feature> features = lineRDD
                .mapToPair(line -> {
                    try {
                        List<String> allItemList = Arrays.asList(line.split(inputInfo.getSeparator()));
                        int[] geometryFieldIndexes = readerHelper.getGeometryFieldIndexes();
                        String[] geom = new String[geometryFieldIndexes.length];
                        for (int i = 0; i < geom.length; i++) {
                            geom[i] = allItemList.get(geometryFieldIndexes[i]);
                        }
                        Feature feature = new Feature(GeometryUtil.getGeometryFromText(geom,
                                inputInfo.getGeometryFieldFormat(), inputInfo.getGeometryType()));
                        String[] items = new String[allItemList.size() - geom.length];
                        int index = 0;
                        for (int i = 0; i < allItemList.size(); i++) {
                            if (!ArrayUtil.contains(geometryFieldIndexes, i)) {
                                items[index++] = allItemList.get(i);
                            }
                        }
                        for (int i = 0; i < fields.length; i++) {
                            feature.addAttribute(fields[i], items[i]);
                        }
                        return new Tuple2<>(feature.getFid(), feature);
                    } catch (Exception e) {
                        dataItemErrorCount.add(1L);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .cache();
        long featureCount = features.count();
        log.info("Data Item Error: " + dataItemErrorCount.count());
        return new Layer(features, new LayerMetadata(readerHelper, featureCount));
    }

    @Override
    public Table readToTable() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        // todo
        return null;
    }

    @Override
    public boolean isValid() {
        return StringUtil.hasMoreThanOne(inputInfo.getSource(), inputInfo.getSeparator(), inputInfo.getCharset(),
                inputInfo.getCrs()) && StringUtil.allNotNull(inputInfo.getLayerNames(), inputInfo.getGeometryFieldFormat(),
                inputInfo.getGeometryType(), inputInfo.getGeometryFieldNames());
    }

    @Getter
    public class TextFileReaderHelper extends ReaderHelper {
        private int[] geometryFieldIndexes;

        protected TextFileReaderHelper(String source) {
            super(source);
            super.initAll();
        }

        @Override
        protected void initCharset() {
            super.charset = inputInfo.getCharset();
        }

        @Override
        protected void initReader() { }

        @Override
        protected void initFields() {
            try {
                FsManipulator fsManipulator = FsManipulatorFactory.create(source);
                String filepath = fsManipulator.isFile(source) ? source : fsManipulator.listFiles(source)[0].toString();
                String firstLine = fsManipulator.readToText(filepath, 1, Charset.forName(charset)).get(0);
                List<String> fieldList = Arrays.asList(firstLine.split(inputInfo.getSeparator()));
                if (!inputInfo.getHeader()) {
                    for (int i = 0; i < fieldList.size(); i++) {
                        fieldList.set(i, String.valueOf(i));
                    }
                }
                String[] geometryFieldNames = inputInfo.getGeometryFieldNames();
                this.geometryFieldIndexes = new int[geometryFieldNames.length];
                for (int i = 0; i < geometryFieldNames.length; i++) {
                    this.geometryFieldIndexes[i] = fieldList.indexOf(geometryFieldNames[i]);
                }
                for (String geometryFieldName : geometryFieldNames) {
                    fieldList.remove(geometryFieldName);
                }
                super.fields = new Field[fieldList.size()];
                for (int i = 0; i < fieldList.size(); i++) {
                    super.fields[i] = new Field(fieldList.get(i));
                }
            } catch (IOException e) {
                String msg = "Fail to read header of text file.";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        @Override
        protected void initCrs() {
            super.crs = CrsUtil.getByCode(inputInfo.getCrs());
        }

        @Override
        protected void initGeometryType() {
            // todo: verify the geometry type of text file.
            super.geometryType = inputInfo.getGeometryType();
        }
    }
}
