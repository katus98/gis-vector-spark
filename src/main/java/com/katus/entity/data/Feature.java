package com.katus.entity.data;

import com.katus.constant.GeomConstant;
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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author SUN Katus
 * @version 1.2, 2021-04-08
 */
@Getter
@Setter
public class Feature implements Serializable {
    public static final Feature EMPTY_FEATURE;

    private String fid;
    private LinkedHashMap<Field, Object> attributes;
    private Geometry geometry;

    static {
        EMPTY_FEATURE = new Feature("EMPTY_FEATURE", new LinkedHashMap<>(), GeomConstant.EMPTY_GEOMETRY);
    }

    public Feature() {
        this(UUID.randomUUID().toString(), new LinkedHashMap<>());
    }

    public Feature(String fid, LinkedHashMap<Field, Object> attributes) {
        this(fid, attributes, null);
    }

    public Feature(Geometry geometry) {
        this(UUID.randomUUID().toString(), new LinkedHashMap<>(), geometry);
    }

    public Feature(Feature feature) {
        this(feature.getFid(), feature.getAttributes(), feature.getGeometry());
    }

    public Feature(String fid, LinkedHashMap<Field, Object> attributes, Geometry geometry) {
        this.fid = fid;
        this.attributes = new LinkedHashMap<>();
        this.attributes.putAll(attributes);
        this.geometry = geometry;
    }

    public boolean hasGeometry() {
        return geometry != null && !geometry.isEmpty();
    }

    public Object getAttribute(Field key) {
        return attributes.get(key);
    }

    public Number getAttributeToNumber(Field key) {
        Number result;
        switch (key.getType()) {
            case DECIMAL: case INTEGER: case INTEGER64:
                result = (Number) getAttribute(key);
                break;
            case DATE:
                result = ((Date) getAttribute(key)).getTime();
                break;
            case TEXT:
                try {
                    result = Double.valueOf((String) getAttribute(key));
                } catch (NumberFormatException e) {
                    result = null;
                }
                break;
            default:
                result = null;
        }
        return result;
    }

    public void removeAttribute(Field key) {
        attributes.remove(key);
    }

    public void setAttribute(Field key, Object value) {
        this.attributes.put(key, value);
    }

    public void addAttributes(LinkedHashMap<Field, Object> attributes) {
        this.attributes.putAll(attributes);
    }

    public void transform(CoordinateReferenceSystem oriCrs, CoordinateReferenceSystem tarCrs) throws FactoryException, TransformException {
        MathTransform mt = CRS.findMathTransform(oriCrs, tarCrs);
        this.geometry = JTS.transform(this.geometry, mt);
    }

    public String showAttributes() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Field, Object> entry : attributes.entrySet()) {
            builder.append(entry.getValue()).append("\t");
        }
        if (attributes.keySet().size() > 0) builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public String toString() {
        return fid + "\t" + showAttributes() + "\t" + geometry.toText();
    }
}
