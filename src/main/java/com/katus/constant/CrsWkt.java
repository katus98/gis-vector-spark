package com.katus.constant;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-12
 */
public final class CrsWkt {
    public static final String WKT_3857;
    public static final String WKT_4326;
    public static final String WKT_4490;
    public static final String WKT_4528;

    static {
        WKT_3857 = "PROJCS[\n" +
                "    \"WGS 84 / Pseudo-Mercator\",\n" +
                "    GEOGCS[\n" +
                "        \"WGS 84\",\n" +
                "        DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],\n" +
                "        PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\n" +
                "        UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],\n" +
                "        AUTHORITY[\"EPSG\",\"4326\"]\n" +
                "    ],\n" +
                "    PROJECTION[\"Mercator_1SP\"],\n" +
                "    PARAMETER[\"central_meridian\",0],\n" +
                "    PARAMETER[\"scale_factor\",1],\n" +
                "    PARAMETER[\"false_easting\",0],\n" +
                "    PARAMETER[\"false_northing\",0],\n" +
                "    UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\n" +
                "    AXIS[\"X\",EAST],\n" +
                "    AXIS[\"Y\",NORTH],\n" +
                "    EXTENSION[\"PROJ4\",\"+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs\"],\n" +
                "    AUTHORITY[\"EPSG\",\"3857\"]\n" +
                "]";
        WKT_4326 = "GEOGCS[\n" +
                "    \"WGS 84\",\n" +
                "    DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],\n" +
                "    PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\n" +
                "    UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],\n" +
                "    AUTHORITY[\"EPSG\",\"4326\"]\n" +
                "]";
        WKT_4490 = "GEOGCS[\n" +
                "    \"China Geodetic Coordinate System 2000\",\n" +
                "    DATUM[\"China_2000\",SPHEROID[\"CGCS2000\",6378137,298.257222101,AUTHORITY[\"EPSG\",\"1024\"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY[\"EPSG\",\"1043\"]],\n" +
                "    PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\n" +
                "    UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],\n" +
                "    AUTHORITY[\"EPSG\",\"4490\"]\n" +
                "]";
        WKT_4528 = "PROJCS[\n" +
                "    \"CGCS2000 / 3-degree Gauss-Kruger zone 40\",\n" +
                "    GEOGCS[\n" +
                "        \"China Geodetic Coordinate System 2000\",\n" +
                "        DATUM[\"China_2000\",SPHEROID[\"CGCS2000\",6378137,298.257222101,AUTHORITY[\"EPSG\",\"1024\"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY[\"EPSG\",\"1043\"]],\n" +
                "        PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],\n" +
                "        UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],\n" +
                "        AUTHORITY[\"EPSG\",\"4490\"]\n" +
                "    ],\n" +
                "    PROJECTION[\"Transverse_Mercator\"],\n" +
                "    PARAMETER[\"latitude_of_origin\",0],\n" +
                "    PARAMETER[\"central_meridian\",120],\n" +
                "    PARAMETER[\"scale_factor\",1],\n" +
                "    PARAMETER[\"false_easting\",40500000],\n" +
                "    PARAMETER[\"false_northing\",0],\n" +
                "    UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],\n" +
                "    EXTENSION[\"PROJ4\",\"+proj=tmerc +lat_0=0 +lon_0=120 +k=1 +x_0=40500000 +y_0=0 +ellps=GRS80 +units=m +no_defs \"],\n" +
                "    AUTHORITY[\"EPSG\",\"4528\"]\n" +
                "]";
    }
}
