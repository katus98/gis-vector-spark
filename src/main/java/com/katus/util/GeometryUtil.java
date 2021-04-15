package com.katus.util;

import com.katus.constant.GeomConstant;
import com.katus.constant.GeometryFieldFormat;
import com.katus.constant.GeometryType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.2, 2021-04-13
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

    public static Geometry breakByDimension(Geometry geometry, int dimension) {
        if (geometry.isEmpty()) return geometry;
        Geometry unionGeom;
        int d = getDimensionOfGeomType(geometry);
        if (d == 3) {   // GeometryCollection
            List<Geometry> geometries = new ArrayList<>();
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                Geometry geometryN = geometry.getGeometryN(i);
                if (getDimensionOfGeomType(geometryN) == dimension && !geometryN.isEmpty()) {
                    geometries.add(geometryN);
                }
            }
            if (geometries.isEmpty()) {
                unionGeom = getEmptyGeometryByDimension(dimension);
            } else {
                unionGeom = geometries.stream().reduce(Geometry::union).get();
            }
        } else if (d == dimension) {
            unionGeom = geometry;
        } else {
            unionGeom = getEmptyGeometryByDimension(dimension);
        }
        return unionGeom;
    }

    public static Geometry getEmptyGeometryByDimension(int dimension) {
        Geometry geometry;
        switch (dimension) {
            case 0:
                geometry = GeomConstant.EMPTY_POINT;
                break;
            case 1:
                geometry = GeomConstant.EMPTY_LINESTRING;
                break;
            case 2:
                geometry = GeomConstant.EMPTY_POLYGON;
                break;
            case 3:
                geometry = GeomConstant.EMPTY_GEOMETRYCOLLECTION;
                break;
            default:
                geometry = GeomConstant.EMPTY_GEOMETRY;
        }
        return geometry;
    }

    public static Geometry getGeometryFromText(String[] text, GeometryFieldFormat format, GeometryType type) throws ParseException {
        Geometry geometry = GeomConstant.EMPTY_GEOMETRY;
        if (text.length == 0) {
            return geometry;
        }
        WKTReader wktReader = new WKTReader();
        switch (format) {
            case WKB:
                WKBReader wkbReader = new WKBReader();
                geometry = wkbReader.read(HexUtil.hexStringToBytes(text[0]));
                break;
            case WKT:
                geometry = wktReader.read(text[0]);
                break;
            case COORDINATES:
                switch (text.length) {
                    case 1:   // Coordinate of points in one field. Need geometry type.
                        if (type.equals(GeometryType.POLYGON)) {
                            geometry = wktReader.read(String.format("POLYGON ((%s))", text[0]));
                        } else {
                            geometry = wktReader.read(String.format("%s (%s)", type.name(), text[0]));
                        }
                        break;
                    case 2:   // Point: lat, lon
                        geometry = wktReader.read(String.format("POINT (%s %s)", text[0], text[1]));
                        break;
                    case 4:   // OD: sLat, sLon, eLat, eLon
                        geometry = wktReader.read(String.format("LINESTRING (%s %s,%s %s)", text[0], text[1], text[2], text[3]));
                        break;
                }
        }
        return geometry;
    }

    @Deprecated
    public static Geometry getGeometryFromText(String[] text, Boolean isWkt, String geometryType) throws ParseException {
        WKTReader wktReader = new WKTReader();
        Geometry geometry;
        switch (text.length) {
            case 1:
                if (isWkt) {   // wkt
                    geometry = wktReader.read(text[0]);
                } else {   // Coordinate of points in one field. Need geometry type, default LineString.
                    if (geometryType.equalsIgnoreCase("Polygon")) {
                        geometry = wktReader.read(String.format("POLYGON ((%s))", text[0]));
                    } else {
                        geometry = wktReader.read(String.format("%s (%s)", geometryType.toUpperCase(), text[0]));
                    }
                }
                break;
            case 2:   // lat, lon
                geometry = wktReader.read(String.format("POINT (%s %s)", text[0], text[1]));
                break;
            case 4:   // OD: sLat, sLon, eLat, eLon
                geometry = wktReader.read(String.format("LINESTRING (%s %s,%s %s)", text[0], text[1], text[2], text[3]));
                break;
            default:
                geometry = GeomConstant.EMPTY_GEOMETRY;
        }
        return geometry;
    }
}
