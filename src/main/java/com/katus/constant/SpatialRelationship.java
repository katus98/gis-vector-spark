package com.katus.constant;

import java.io.Serializable;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
public enum SpatialRelationship implements Serializable {
    INTERSECTS,
    OVERLAPS,
    CONTAINS,
    WITHIN,
    EQUALS,
    CROSSES,
    TOUCHES;

    public String getMethodName() {
        return this.name().toLowerCase();
    }
}
