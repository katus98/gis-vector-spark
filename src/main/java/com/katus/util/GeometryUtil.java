package com.katus.util;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
public final class GeometryUtil {
    public static int getDimensionOfGeomType(String geometryType) {
        int dimension = -1;
        geometryType = geometryType.toLowerCase();
        switch (geometryType) {
            case "point": case "multipoint":
                dimension = 0;
                break;
            case "linestring": case "multilinestring": case "linearring":
                dimension = 1;
                break;
            case "polygon": case "multipolygon":
                dimension = 2;
                break;
            case "geometrycollection":
                dimension = 3;
                break;
        }
        return dimension;
    }

    public static int getDimensionOfGeomType(Geometry geometry) {
        return getDimensionOfGeomType(geometry.getGeometryType());
    }

    public static Geometry breakGeometryCollectionByDimension(Geometry geometry, int dimension) throws ParseException {
        Geometry unionGeom;
        if (getDimensionOfGeomType(geometry) == 3 && !geometry.isEmpty()) {   // GeometryCollection
            GeometryCollection collection = (GeometryCollection) geometry;
            List<Geometry> geometries = new ArrayList<>();
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                if (getDimensionOfGeomType(collection.getGeometryN(i)) == dimension) {
                    geometries.add(collection.getGeometryN(i));
                }
            }
            if (geometries.isEmpty()) {
                unionGeom = new WKTReader().read("POLYGON EMPTY");
            } else {
                unionGeom = geometries.get(0);
                for (int i = 1; i < geometries.size(); i++) {
                    unionGeom = unionGeom.union(geometries.get(i));
                }
            }
        } else {
            unionGeom = geometry;
        }
        return unionGeom;
    }
}
