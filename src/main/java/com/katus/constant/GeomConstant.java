package com.katus.constant;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
public final class GeomConstant {
    public static Geometry EMPTY_GEOM;
    public static Geometry EMPTY_POINT;
    public static Geometry EMPTY_LINESTRING;
    public static Geometry EMPTY_POLYGON;
    static {
        try {
            WKTReader reader = new WKTReader();
            EMPTY_POINT = reader.read("POINT EMPTY");
            EMPTY_LINESTRING = reader.read("LINESTRING EMPTY");
            EMPTY_POLYGON = reader.read("POLYGON EMPTY");
            EMPTY_GEOM = EMPTY_POLYGON;
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
