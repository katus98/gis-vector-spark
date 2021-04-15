package com.katus.util;

import com.katus.constant.CrsWkt;
import lombok.extern.slf4j.Slf4j;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Crs Utils, only support 4326(default), 3857, 4490 and 4528.
 * @author SUN Katus
 * @version 1.2, 2021-04-12
 */
@Slf4j
public final class CrsUtil {

    public static CoordinateReferenceSystem getByOGCWkt(String wkt) throws FactoryException {
        return CRS.parseWKT(wkt);
    }

    public static CoordinateReferenceSystem getByCode(String crsCode) {
        try {
            switch (crsCode) {
                case "4326":
                    return CRS.parseWKT(CrsWkt.WKT_4326);
                case "3857":
                    return CRS.parseWKT(CrsWkt.WKT_3857);
                case "4490":
                    return CRS.parseWKT(CrsWkt.WKT_4490);
                case "4528":
                    return CRS.parseWKT(CrsWkt.WKT_4528);
                default:
                    String msg = "CRS: " + crsCode + " Not Support!";
                    log.error(msg);
                    throw new RuntimeException(msg);
            }
        } catch (FactoryException e) {
            log.warn("Fail to parse crs wkt.", e);
            throw new RuntimeException(e);
        }
    }

    public static CoordinateReferenceSystem getByESRIWkt(String wkt) throws FactoryException {
        String crsCode;
        if (wkt.contains("WGS_1984_Web_Mercator_Auxiliary_Sphere")) {
            crsCode = "3857";
        } else if (wkt.contains("China Geodetic Coordinate System 2000")) {
            crsCode = "4490";
        } else if (wkt.contains("CGCS2000_3_degree_Gauss_Kruger_zone_40")) {
            crsCode = "4528";
        } else {
            crsCode = "4326";
        }
        return getByCode(crsCode);
    }
}
