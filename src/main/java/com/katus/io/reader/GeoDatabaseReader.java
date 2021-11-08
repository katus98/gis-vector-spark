package com.katus.io.reader;

import com.katus.entity.LayerMetadata;
import com.katus.entity.data.*;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import com.katus.util.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.FeatureDefn;
import org.gdal.ogr.Geometry;
import org.gdal.ogr.ogr;
import org.locationtech.jts.io.ParseException;
import scala.Tuple2;

import java.util.*;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-14
 * @since 2.0
 */
@Slf4j
public class GeoDatabaseReader extends Reader {
    private final List<String> layerPathList;

    protected GeoDatabaseReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
        this.layerPathList = new ArrayList<>();
        String[] layerNames = inputInfo.getLayerNames();
        for (String layerName : layerNames) {
            this.layerPathList.add(inputInfo.getSource() + ":" + layerName);
        }
    }

    static {
        gdal.AllRegister();
        ogr.RegisterAll();
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaPairRDD<String, Feature> featuresWithInfo = jsc.parallelize(layerPathList)
                .flatMapToPair(layerPath -> {
                    String path = layerPath.substring(0, layerPath.lastIndexOf(":"));
                    String layerName = layerPath.substring(layerPath.lastIndexOf(":") + 1);
                    GeoDatabaseReaderHelper readerHelper = new GeoDatabaseReaderHelper(path, layerName);
                    List<Tuple2<String, Feature>> result = new ArrayList<>();
                    Feature feature = readerHelper.next();
                    while (feature != null) {
                        result.add(new Tuple2<>(feature.getFid(), feature));
                        feature = readerHelper.next();
                    }
                    MetaFeature metaFeature = new MetaFeature();
                    metaFeature.setReaderHelper(readerHelper);
                    result.add(new Tuple2<>(MetaFeature.META_ID, metaFeature));
                    return result.iterator();
                })
                .repartition(jsc.defaultParallelism())
                .cache();
        long featureCount = featuresWithInfo.count() - layerPathList.size();
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
        return StringUtil.hasMoreThanOne(inputInfo.getLayerNames()) && Objects.nonNull(inputInfo.getGeometryType())
                && Objects.nonNull(inputInfo.getCharset()) && Objects.nonNull(inputInfo.getCrs());
    }

    @Getter
    public class GeoDatabaseReaderHelper extends ReaderHelper {
        private final String layerName;
        private transient org.gdal.ogr.Layer layer;

        public GeoDatabaseReaderHelper(String source, String layerName) {
            super(source);
            super.source = source.startsWith("file://") ?
                    source.substring(7, source.length()-4) :
                    source.substring(0, source.length()-4);
            this.layerName = layerName;
            super.initAll();
        }

        @Override
        protected void initCharset() {
            super.charset = inputInfo.getCharset();
        }

        @Override
        protected void initReader() {
            DataSource ds = ogr.Open(source);
            Objects.requireNonNull(ds);
            try {
                this.layer = ds.GetLayerByName(layerName);
            } catch (Exception e) {
                String msg = "Fail to read File GeoDatabase!";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }
        }

        @Override
        protected void initFields() {
            FeatureDefn ftd = layer.GetLayerDefn();
            int fieldCount = ftd.GetFieldCount();
            super.fields = new Field[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                super.fields[i] = new Field(ftd.GetFieldDefn(i).GetName(), ftd.GetFieldDefn(i).GetFieldType());
            }
        }

        @Override
        protected void initCrs() {
            try {
                super.crs = CrsUtil.getByOGCWkt(layer.GetSpatialRef().ExportToWkt());
            } catch (Exception e) {
                String msg = "Fail to read spatial reference of the geo database.";
                log.warn(msg, e);
                super.crs = CrsUtil.getByCode(inputInfo.getCrs());
            }
        }

        @Override
        protected void initGeometryType() {
            // todo: verify the geometry type of geo database.
            super.geometryType = inputInfo.getGeometryType();
        }

        public Feature next() throws ParseException {
            Feature feature = null;
            org.gdal.ogr.Feature ft;
            if ((ft = layer.GetNextFeature()) != null) {
                feature = new Feature(((Long) ft.GetFID()).toString());
                for (Field field : fields) {
                    feature.addAttribute(field, ft.GetFieldAsString(field.getName())
                            .replaceAll("[\r\n]", "").replace("\t", " "));
                }
                Geometry g = ft.GetGeometryRef();
                feature.setGeometry(GeometryUtil.getGeometryFromText(new String[]{g.ExportToWkt()},
                        inputInfo.getGeometryFieldFormat(), geometryType));
            }
            return feature;
        }
    }
}
