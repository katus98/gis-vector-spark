package com.katus;

import com.katus.util.CrsUtil;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
public class GtTest {
    public static void main(String[] args) throws Exception {
        testCrs();
    }

    public static void test0() throws ParseException {
        String wktP1 = "POLYGON ((1 1, 1 2, 2 2, 1 1))";
        String wktP2 = "POLYGON ((2 1, 3 1, 3 2, 2 1))";
        String wktP3 = "POLYGON ((1 1, 1 3, 2 0, 1 1))";
        String wktP4 = "POLYGON EMPTY";
        String wktP5 = "LINESTRING EMPTY";
        String wktP6 = "POINT EMPTY";
        WKTReader reader = new WKTReader();
        Geometry p1 = reader.read(wktP1);
        Geometry p2 = reader.read(wktP2);
        Geometry p3 = reader.read(wktP3);
        Geometry p4 = reader.read(wktP4);
        Geometry p5 = reader.read(wktP5);
        Geometry p6 = reader.read(wktP6);
        System.out.println(p4.isEmpty());
        System.out.println(p5.isEmpty());
        System.out.println(p6.isEmpty());
    }

    public static void test() {
        Number num = 12L;
        System.out.println(num.getClass());
    }

    public static void testCrs() throws FactoryException {
        System.out.println(CrsUtil.getByCode("3857").getName().toString());
        System.out.println(CrsUtil.getByCode("4326").getName().toString());
        System.out.println(CrsUtil.getByCode("4490").getName().toString());
        System.out.println(CrsUtil.getByCode("4528").getName().toString());
        CoordinateReferenceSystem crs = CrsUtil.getByCode("4528");
    }
}
