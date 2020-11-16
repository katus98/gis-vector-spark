package com.katus.constant;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-12
 */
public final class CrsWkt {
    public static final String WKT_4326 = "GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\"," +
            "SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]]," +
            "AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]]," +
            "UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4326\"]]";
    public static final String WKT_3857 = "PROJCS[\"WGS 84 / Pseudo-Mercator\",\n" +
            "    GEOGCS[\"WGS 84\",\n" +
            "        DATUM[\"WGS_1984\",\n" +
            "            SPHEROID[\"WGS 84\",6378137,298.257223563,\n" +
            "                AUTHORITY[\"EPSG\",\"7030\"]],\n" +
            "            AUTHORITY[\"EPSG\",\"6326\"]],\n" +
            "        PRIMEM[\"Greenwich\",0,\n" +
            "            AUTHORITY[\"EPSG\",\"8901\"]],\n" +
            "        UNIT[\"degree\",0.0174532925199433,\n" +
            "            AUTHORITY[\"EPSG\",\"9122\"]],\n" +
            "        AUTHORITY[\"EPSG\",\"4326\"]],\n" +
            "    PROJECTION[\"Mercator_1SP\"],\n" +
            "    PARAMETER[\"central_meridian\",0],\n" +
            "    PARAMETER[\"scale_factor\",1],\n" +
            "    PARAMETER[\"false_easting\",0],\n" +
            "    PARAMETER[\"false_northing\",0],\n" +
            "    UNIT[\"metre\",1,\n" +
            "        AUTHORITY[\"EPSG\",\"9001\"]],\n" +
            "    AXIS[\"X\",EAST],\n" +
            "    AXIS[\"Y\",NORTH],\n" +
            "    AUTHORITY[\"EPSG\",\"3857\"]]";
}
