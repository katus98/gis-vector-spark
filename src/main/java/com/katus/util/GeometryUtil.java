package com.katus.util;

import org.locationtech.jts.geom.Geometry;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
public final class GeometryUtil {
    public static int getDimensionOfGeomType(String geometryType) {
        int dimension = 0;
        geometryType = geometryType.toLowerCase();
        switch (geometryType) {
            case "point": case "multipoint":
                dimension = 1;
                break;
            case "linestring": case "multilinestring":
                dimension = 2;
                break;
            case "polygon": case "multipolygon":
                dimension = 3;
                break;
        }
        return dimension;
    }

    public static int getDimensionOfGeomType(Geometry geometry) {
        return getDimensionOfGeomType(geometry.getGeometryType());
    }
}
