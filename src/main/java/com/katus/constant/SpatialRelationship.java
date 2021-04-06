package com.katus.constant;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.1, 2021-04-06
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

    public static boolean contains(String... names) {
        for (String name : names) {
            boolean flag = false;
            for (SpatialRelationship relationship : SpatialRelationship.values()) {
                if (relationship.name().equals(name.toUpperCase())) {
                    flag = true;
                    break;
                }
            }
            if (!flag) return false;
        }
        return true;
    }
}
