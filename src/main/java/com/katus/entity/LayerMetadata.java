package com.katus.entity;

import com.katus.entity.data.Field;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-12
 */
@Getter
@Setter
@Slf4j
public class LayerMetadata implements Serializable {
    // todo
    private String[] fieldNames;
    private Field[] fields;
    private CoordinateReferenceSystem crs;
    private String geometryType;
    private Long featureCount;

    public LayerMetadata(Field[] fields, CoordinateReferenceSystem crs, String geometryType) {
        this(fields, crs, geometryType, -1L);
    }

    public LayerMetadata(Field[] fields, CoordinateReferenceSystem crs, String geometryType, Long featureCount) {
        this.fields = fields;
        this.crs = crs;
        this.geometryType = geometryType;
        this.featureCount = featureCount;
    }

    public Field getFieldByName(String name) {
        for (Field field : fields) {
            if (field.getName().equals(name)) return field;
        }
        String msg = "Field " + name + " does not exist!";
        log.error(msg);
        throw new RuntimeException(msg);
    }

    @Override
    public String toString() {
        return "******Metadata******\n" +
                "------Field Name------\n" + Arrays.toString(fieldNames) + "\n" +
                "------Coordinate Reference System (WKT)------\n" + crs + "\n" +
                "------Geometry Type------\n" + geometryType + "\n" +
                "------Feature Count------\n" + featureCount + "\n";
    }
}
