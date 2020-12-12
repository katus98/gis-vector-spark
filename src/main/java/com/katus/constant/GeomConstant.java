package com.katus.constant;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-18
 */
public final class GeomConstant {
    public static Geometry EMPTY_GEOMETRY;
    public static Geometry EMPTY_POINT;
    public static Geometry EMPTY_LINESTRING;
    public static Geometry EMPTY_POLYGON;
    public static Geometry EMPTY_GEOMETRYCOLLECTION;
    static {
        try {
            WKTReader reader = new WKTReader();
            EMPTY_POINT = reader.read("POINT EMPTY");
            EMPTY_LINESTRING = reader.read("LINESTRING EMPTY");
            EMPTY_POLYGON = reader.read("POLYGON EMPTY");
            EMPTY_GEOMETRYCOLLECTION = reader.read("GEOMETRYCOLLECTION EMPTY");
            EMPTY_GEOMETRY = EMPTY_POINT;
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
