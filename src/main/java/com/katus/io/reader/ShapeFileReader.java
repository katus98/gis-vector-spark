package com.katus.io.reader;

import com.katus.entity.Feature;
import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-04
 */
@Getter
public class ShapeFileReader implements Closeable {
    private final String filename;
    private String charset;
    private String[] fieldNames;
    private String crs;
    private FeatureIterator<SimpleFeature> reader;
    private ShapefileDataStore shpDataStore;

    public ShapeFileReader(String filename) {
        filename = filename.replace("file://", "");
        this.filename = filename.endsWith(".shp") ? filename.substring(0, filename.length()-4) : filename;
        initAll();
    }

    private void initAll() {
        initCharset();
        initFields();
        initCrs();
        initReader();
    }

    private void initCharset() {
        try {
            FsManipulator fsManipulator = FsManipulatorFactory.create();
            if (fsManipulator.exists(filename + ".cpg")) {
                this.charset = fsManipulator.readToText(filename + ".cpg").get(0).trim().toUpperCase();
            } else {
                this.charset = "UTF-8";
            }
        } catch (IOException e) {
            e.printStackTrace();
            this.charset = "UTF-8";
        }
    }

    private void initFields() {
        DbaseFileReader dbfReader;
        try {
            dbfReader = new DbaseFileReader(new ShpFiles(filename + ".shp"), false, Charset.forName(charset));
            DbaseFileHeader header = dbfReader.getHeader();
            int numFields = header.getNumFields();
            String[] fields = new String[numFields];
            for (int i = 0; i < numFields; i++) {
                fields[i] = header.getFieldName(i);
            }
            this.fieldNames = fields;
            dbfReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Shape file header error!");
        }
    }

    private void initCrs() {
        try {
            FsManipulator fsManipulator = FsManipulatorFactory.create();
            this.crs = fsManipulator.readToText(filename + ".prj").get(0).trim();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Shape file crs error!");
        }
    }

    private void initReader() {
        File file = new File(filename + ".shp");
        try {
            this.shpDataStore = new ShapefileDataStore(file.toURI().toURL());
            this.shpDataStore.setCharset(Charset.forName(charset));
            String typeName = this.shpDataStore.getTypeNames()[0]; // 获取第一层图层名
            SimpleFeatureSource featureSource = this.shpDataStore.getFeatureSource(typeName);
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = featureSource.getFeatures();
            this.reader = collection.features();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Shape file read error!");
        }
    }

    public Feature next() {
        Feature feature = null;
        if (reader.hasNext()) {
            SimpleFeature sf = reader.next();
            feature = new Feature((Geometry) sf.getDefaultGeometry());
            for (String fieldName : fieldNames) {
                feature.setAttribute(fieldName, sf.getAttribute(fieldName));
            }
        }
        return feature;
    }

    @Override
    public void close() {
        if (reader != null) reader.close();
        if (this.shpDataStore != null) this.shpDataStore.dispose();
    }
}
