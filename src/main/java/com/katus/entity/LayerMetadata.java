package com.katus.entity;

import lombok.Getter;
import lombok.Setter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-12
 */
@Getter
@Setter
public class LayerMetadata implements Serializable {
    String[] fieldNames;
    private CoordinateReferenceSystem crs;
    String geometryType;
    Long featureCount;

    public LayerMetadata(String[] fieldNames, CoordinateReferenceSystem crs, String geometryType) {
        this(fieldNames, crs, geometryType, -1L);
    }

    public LayerMetadata(String[] fieldNames, CoordinateReferenceSystem crs, String geometryType, Long featureCount) {
        this.fieldNames = fieldNames;
        this.crs = crs;
        this.geometryType = geometryType;
        this.featureCount = featureCount;
    }

    @Override
    public String toString() {
        return "******Metadata******\n" +
                "------Field Name------\nfid, " + Arrays.toString(fieldNames) + "\n" +
                "------Coordinate Reference System (WKT)------\n" + crs + "\n" +
                "------Geometry Type------\n" + geometryType + "\n" +
                "------Feature Count------\n" + featureCount + "\n";
    }
}
