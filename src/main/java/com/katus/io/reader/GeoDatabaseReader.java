package com.katus.io.reader;

import com.katus.entity.Feature;
import com.katus.util.CrsUtil;
import com.katus.util.GeometryUtil;
import lombok.Getter;
import org.gdal.gdal.gdal;
import org.gdal.ogr.*;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-23
 * @since 1.2
 */
@Getter
public class GeoDatabaseReader {
    private final String path;
    private final String layerName;
    private Layer layer;
    private String[] fieldNames;
    private CoordinateReferenceSystem crs = null;
    private static final Boolean WITH_FID = false;

    public GeoDatabaseReader(String pathWithLayerName) {
        pathWithLayerName = pathWithLayerName.replace("file://", "");
        int index = pathWithLayerName.lastIndexOf(":");
        this.path = pathWithLayerName.substring(0, index);
        this.layerName = pathWithLayerName.substring(index+1);
        initAll();
    }

    public GeoDatabaseReader(String path, String layerName) {
        this.path = path.replace("file://", "");
        this.layerName = layerName;
        initAll();
    }

    static {
        gdal.AllRegister();
        ogr.RegisterAll();
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
    }
    
    private void initAll() {
        DataSource ds = ogr.Open(path);
        ds = Objects.requireNonNull(ds);
        try {
            this.layer = ds.GetLayerByName(layerName);
            initFields();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Read File GeoDatabase Error!");
        }
    }

    private void initFields() {
        FeatureDefn ftd = layer.GetLayerDefn();
        int fieldCount = ftd.GetFieldCount();
        List<String> fields = new ArrayList<>();
        if (WITH_FID) fields.add("FID");
        for (int i = 0; i < fieldCount; i++) {
            fields.add(ftd.GetFieldDefn(i).GetName());
        }
        this.fieldNames = new String[fields.size()];
        fields.toArray(this.fieldNames);
    }

    public Feature next() throws ParseException, FactoryException {
        Feature feature = null;
        org.gdal.ogr.Feature ft;
        if ((ft = layer.GetNextFeature()) != null) {
            feature = new Feature();
            if (WITH_FID) {
                feature.setAttribute("FID", ft.GetFID());
            }
            for (String fieldName : fieldNames) {
                if (fieldName.equals("FID") && feature.getAttributes().containsKey("FID")) continue;
                feature.setAttribute(fieldName, ft.GetFieldAsString(fieldName).replaceAll("[\r\n]", "").replace("\t", " "));
            }
            Geometry g = ft.GetGeometryRef();
            if (crs == null) this.crs = CrsUtil.getByOGCWkt(g.GetSpatialReference().ExportToWkt());
            feature.setGeometry(GeometryUtil.getGeometryFromText(new String[]{g.ExportToWkt()}, true, ""));
        }
        return feature;
    }
}
