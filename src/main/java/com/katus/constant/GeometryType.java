package com.katus.constant;

import lombok.Getter;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
@Getter
public enum GeometryType {
    NONE(-1),
    GEOMETRYCOLLECTION(3),
    POINT(0),
    MULTIPOINT(0),
    LINESTRING(1),
    MULTILINESTRING(1),
    POLYGON(2),
    MULTIPOLYGON(2),
    LINEAR_RING(1);

    private final int dimension;

    GeometryType(int dimension) {
        this.dimension = dimension;
    }

    public GeometryType getBasicByDimension() {
        switch (dimension) {
            case 0:
                return POINT;
            case 1:
                return LINESTRING;
            case 2:
                return POLYGON;
            case 3:
                return GEOMETRYCOLLECTION;
            default:
                return NONE;
        }
    }

    public static GeometryType getByShpType(String type) {
        // todo
        switch (type.toUpperCase()) {
            case "":
            default:
        }
        return GeometryType.valueOf(type.toUpperCase());
    }
}
