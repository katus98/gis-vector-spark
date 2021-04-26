package com.katus.entity;

import com.katus.constant.GeometryType;
import com.katus.entity.data.Field;
import com.katus.io.reader.Reader;
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
    private String name;
    private Field[] fields;
    private CoordinateReferenceSystem crs;
    private GeometryType geometryType;
    private long featureCount;

    public LayerMetadata(Reader.ReaderHelper helper) {
        this(helper, -1L);
    }

    public LayerMetadata(Reader.ReaderHelper helper, long featureCount) {
        this(helper.getName(), helper.getFields(), helper.getCrs(), helper.getGeometryType(), featureCount);
    }

    public LayerMetadata(String name, Field[] fields, CoordinateReferenceSystem crs, GeometryType geometryType, long featureCount) {
        this.name = name;
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
                "------Field Name------\n" + Arrays.toString(fields) + "\n" +
                "------Coordinate Reference System (WKT)------\n" + crs + "\n" +
                "------Geometry Type------\n" + geometryType + "\n" +
                "------Feature Count------\n" + featureCount + "\n";
    }

    public LayerMetadata copy() {
        LayerMetadata copiedMetadata;
        Field[] fields = new Field[this.getFields().length];
        for (int i = 0; i < this.getFields().length; i++) {
            fields[i] = this.getFields()[i].copy();
        }
        try {
            copiedMetadata = (LayerMetadata) this.clone();
            copiedMetadata.setFields(fields);
        } catch (CloneNotSupportedException e) {
            copiedMetadata = new LayerMetadata(this.getName(), fields,
                    this.getCrs(), this.getGeometryType(), this.getFeatureCount());
        }
        return copiedMetadata;
    }
}
