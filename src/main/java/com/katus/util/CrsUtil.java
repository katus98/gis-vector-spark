package com.katus.util;

import com.katus.constant.CrsWkt;
import lombok.extern.slf4j.Slf4j;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Crs Utils, only support 4326(default) and 3857.
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-12
 */
@Slf4j
public final class CrsUtil {
    public static CoordinateReferenceSystem getByWKT(String wkt) throws FactoryException {
        return CRS.parseWKT(wkt);
    }

    public static CoordinateReferenceSystem getByCode(String crsCode) throws FactoryException {
        switch (crsCode) {
            case "4326":
                return CRS.parseWKT(CrsWkt.WKT_4326);
            case "3857":
                return CRS.parseWKT(CrsWkt.WKT_3857);
            default:
                String msg = "CRS: " + crsCode + " Not Support!";
                log.error(msg);
                throw new RuntimeException(msg);
        }
    }

    public static CoordinateReferenceSystem getByESRIWkt(String wkt) throws FactoryException {
        String crsCode;
        if (wkt.contains("WGS_1984_Web_Mercator_Auxiliary_Sphere")) {
            crsCode = "3857";
        } else {
            crsCode = "4326";
        }
        return getByCode(crsCode);
    }
}
