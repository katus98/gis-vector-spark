package com.katus.util;

import org.locationtech.jts.geom.Envelope;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-12
 * @since 1.2
 */
public final class TileUtil {
    public static String getPolygonWkt(Envelope range) {
        return "POLYGON ((" + range.getMinX() + " " + range.getMinY() + "," +
                range.getMinX() + " " + range.getMaxY() + "," +
                range.getMaxX() + " " + range.getMaxY() + "," +
                range.getMaxX() + " " + range.getMinY() + "," +
                range.getMinX() + " " + range.getMinY() + "))";
    }

    public static String getPolylineWkt(Envelope range) {
        return "LINESTRING (" + range.getMinX() + " " + range.getMinY() + "," +
                range.getMinX() + " " + range.getMaxY() + "," +
                range.getMaxX() + " " + range.getMaxY() + "," +
                range.getMaxX() + " " + range.getMinY() + "," +
                range.getMinX() + " " + range.getMinY() + ")";
    }
}
