package com.katus.io.reader;

import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-21
 */
@Getter
public class TextFileReader implements Serializable {
    private final String pathURI;
    private Boolean hasHeader;
    private Boolean isWkt;
    /*
    If is point with (lon, lat), please new String[]{lon, lat}.
    If is OD with (sLon, sLat, eLon, eLat), please new String[]{sLon, sLat, eLon, eLat}.
    Allow negative index.
     */
    private String[] geometryFields;
    private String separator;
    private String crs;
    private String charset;
    private String[] fieldNames;
    @Setter
    private String geometryType = "LineString";   // Polygon, LineString, Point

    public TextFileReader(String pathURI) {
        this(pathURI, false, true, new String[]{"-1"});
    }

    public TextFileReader(String pathURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields) {
        this(pathURI, hasHeader, isWkt, geometryFields, "\t");
    }

    public TextFileReader(String pathURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator) {
        this(pathURI, hasHeader, isWkt, geometryFields, separator, "4326");
    }

    public TextFileReader(String pathURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator, String crs) {
        this(pathURI, hasHeader, isWkt, geometryFields, separator, crs, "UTF-8");
    }

    public TextFileReader(String pathURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator, String crs, String charset) {
        this.pathURI = pathURI;
        this.hasHeader = hasHeader;
        this.isWkt = isWkt;
        this.geometryFields = geometryFields;
        this.separator = separator;
        this.crs = crs;
        this.charset = charset;
        initFields();
    }

    private void initFields() {
        try {
            FsManipulator fsManipulator = FsManipulatorFactory.create(pathURI);
            String filepath = fsManipulator.isFile(pathURI) ? pathURI : fsManipulator.listFiles(pathURI)[0].toString();
            String firstLine = fsManipulator.readToText(filepath, 1, Charset.forName(charset)).get(0);
            String[] fields = firstLine.split(separator);
            initGeometryFields(fields.length);
            if (hasHeader) {
                this.fieldNames = fields;
            } else {
                this.fieldNames = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    this.fieldNames[i] = String.valueOf(i);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Text file header error!");
        }
    }

    private void initGeometryFields(int fieldLength) {
        if (!hasHeader) {
            for (int i = 0; i < geometryFields.length; i++) {
                int index = Integer.parseInt(geometryFields[i]);
                if (index < 0) {
                    this.geometryFields[i] = String.valueOf(fieldLength + index);
                }
            }
        }
    }
}
