package com.katus.io.reader;

import com.katus.constant.GeometryType;
import com.katus.entity.LayerMetadata;
import com.katus.entity.data.*;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import com.katus.util.CrsUtil;
import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import scala.Tuple2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
@Slf4j
public class ShapeFileReader extends Reader {

    protected ShapeFileReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaPairRDD<String, Feature> featuresWithInfo = jsc.parallelize(Collections.singletonList(inputInfo.getSource()))
                .flatMapToPair(source -> {
                    ShapeFileReaderHelper readerHelper = this.new ShapeFileReaderHelper(source);
                    List<Tuple2<String, Feature>> result = new ArrayList<>();
                    Feature feature = readerHelper.next();
                    while (feature != null) {
                        result.add(new Tuple2<>(feature.getFid(), feature));
                        feature = readerHelper.next();
                    }
                    readerHelper.close();
                    MetaFeature metaFeature = new MetaFeature();
                    metaFeature.setReaderHelper(readerHelper);
                    result.add(new Tuple2<>(MetaFeature.META_ID, metaFeature));
                    return result.iterator();
                })
                .repartition(jsc.defaultParallelism())
                .cache();
        long featureCount = featuresWithInfo.count() - 1;
        ReaderHelper readerHelper = ((MetaFeature) featuresWithInfo.filter(pairItem -> pairItem._1().equals(MetaFeature.META_ID)).first()._2()).getReaderHelper();
        JavaPairRDD<String, Feature> features = featuresWithInfo.filter(pairItem -> !pairItem._1().equals(MetaFeature.META_ID)).cache();
        featuresWithInfo.unpersist();
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
        String source = inputInfo.getSource();
        return !source.isEmpty() && source.endsWith(".shp");
    }

    @Getter
    public class ShapeFileReaderHelper extends ReaderHelper implements Closeable {
        private transient FeatureIterator<SimpleFeature> reader;
        private transient ShapefileDataStore shpDataStore;

        public ShapeFileReaderHelper(String source) {
            super(source);
            super.source = source.startsWith("file://") ?
                    source.substring(7, source.length()-4) :
                    source.substring(0, source.length()-4);
            super.initAll();
        }

        @Override
        protected void initCharset() {
            try {
                FsManipulator fsManipulator = FsManipulatorFactory.create();
                if (fsManipulator.exists(source + ".cpg")) {
                    super.charset = fsManipulator.readToText(source + ".cpg").get(0).trim().toUpperCase();
                } else {
                    super.charset = inputInfo.getCharset();
                }
            } catch (IOException e) {
                String msg = "Fail to read charset file (.cpg).";
                log.warn(msg, e);
                super.charset = inputInfo.getCharset();
            }
        }

        @Override
        protected void initReader() {
            File file = new File(source + ".shp");
            try {
                this.shpDataStore = new ShapefileDataStore(file.toURI().toURL());
                shpDataStore.setCharset(Charset.forName(charset));
                super.name = shpDataStore.getTypeNames()[0]; // 获取第一层图层名
                SimpleFeatureSource featureSource = shpDataStore.getFeatureSource(name);
                FeatureCollection<SimpleFeatureType, SimpleFeature> collection = featureSource.getFeatures();
                this.reader = collection.features();
            } catch (Exception e) {
                String msg = "Fail to read shapefile.";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        @Override
        protected void initFields() {
            DbaseFileReader dbfReader;
            try {
                dbfReader = new DbaseFileReader(new ShpFiles(source + ".shp"),
                        false, Charset.forName(charset));
                DbaseFileHeader header = dbfReader.getHeader();
                int numFields = header.getNumFields();
                super.fields = new Field[numFields];
                for (int i = 0; i < numFields; i++) {
                    super.fields[i] = new Field(header.getFieldName(i), header.getFieldClass(i));
                }
                dbfReader.close();
            } catch (IOException e) {
                String msg = "Fail to read header of shapefile.";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        @Override
        protected void initCrs() {
            try {
                super.crs = shpDataStore.getSchema().getCoordinateReferenceSystem();
            } catch (IOException e) {
                String msg = "Fail to load crs.";
                log.warn(msg, e);
                super.crs = CrsUtil.getByCode(inputInfo.getCrs());
            }
        }

        @Override
        protected void initGeometryType() {
            try {
                super.geometryType = GeometryType.getByShpType(
                        shpDataStore.getSchema().getGeometryDescriptor().getType().getName().toString());
            } catch (IOException e) {
                String msg = "Fail to load geometry type.";
                log.warn(msg, e);
                super.geometryType = inputInfo.getGeometryType();
            }
        }

        public Feature next() {
            Feature feature = null;
            if (reader.hasNext()) {
                SimpleFeature sf = reader.next();
                feature = new Feature((Geometry) sf.getDefaultGeometry());
                for (Field field : fields) {
                    switch (field.getType()) {
                        case TEXT:
                            feature.setAttribute(field, sf.getAttribute(field.getName()).toString());
                            break;
                        case INTEGER:
                            feature.setAttribute(field, Integer.valueOf(sf.getAttribute(field.getName()).toString()));
                            break;
                        case DECIMAL:
                            feature.setAttribute(field, Double.valueOf(sf.getAttribute(field.getName()).toString()));
                            break;
                        default:
                            feature.setAttribute(field, sf.getAttribute(field.getName()));
                    }
                }
            }
            return feature;
        }

        @Override
        public void close() throws IOException {
            if (reader != null) reader.close();
            if (shpDataStore != null) shpDataStore.dispose();
        }
    }
}
