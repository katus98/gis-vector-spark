package com.katus.entity;

import lombok.Getter;
import lombok.Setter;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-04
 */
@Getter
@Setter
public class Feature implements Serializable {
    private String fid;
    private LinkedHashMap<String, Object> attributes;
    private Geometry geometry;

    public Feature() {
        this(UUID.randomUUID().toString(), new LinkedHashMap<>());
    }

    public Feature(String fid, LinkedHashMap<String, Object> attributes) {
        this(fid, attributes, null);
    }

    public Feature(Geometry geometry) {
        this(UUID.randomUUID().toString(), new LinkedHashMap<>(), geometry);
    }

    public Feature(String fid, LinkedHashMap<String, Object> attributes, Geometry geometry) {
        this.fid = fid;
        this.attributes = new LinkedHashMap<>();
        this.attributes.putAll(attributes);
        this.geometry = geometry;
    }

    public boolean hasGeometry() {
        return geometry != null && !geometry.isEmpty();
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        this.attributes.put(key, value);
    }

    public void addAttributes(LinkedHashMap<String, Object> attributes) {
        this.attributes.putAll(attributes);
    }

    public void transform(CoordinateReferenceSystem oriCrs, CoordinateReferenceSystem tarCrs) throws FactoryException, TransformException {
        MathTransform mt = CRS.findMathTransform(oriCrs, tarCrs);
        this.geometry = JTS.transform(this.geometry, mt);
    }

    public String showAttributes() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            builder.append(entry.getValue()).append("\t");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public String toString() {
        return fid + "\t" + showAttributes() + "\t" + geometry.toText();
    }
}
