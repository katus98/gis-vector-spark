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
 * @version 1.0, 2020-11-12
 */
@Getter
public class TextFileReader implements Serializable {
    private final String fileURI;
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

    public TextFileReader(String fileURI) {
        this(fileURI, false, true, new String[]{"-1"});
    }

    public TextFileReader(String fileURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields) {
        this(fileURI, hasHeader, isWkt, geometryFields, "\t");
    }

    public TextFileReader(String fileURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator) {
        this(fileURI, hasHeader, isWkt, geometryFields, separator, "4326");
    }

    public TextFileReader(String fileURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator, String crs) {
        this(fileURI, hasHeader, isWkt, geometryFields, separator, crs, "UTF-8");
    }

    public TextFileReader(String fileURI, Boolean hasHeader, Boolean isWkt, String[] geometryFields, String separator, String crs, String charset) {
        this.fileURI = fileURI;
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
            FsManipulator fsManipulator = FsManipulatorFactory.create(fileURI);
            String firstLine = fsManipulator.readToText(fileURI, 1, Charset.forName(charset)).get(0);
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
