package com.katus.constant;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.1, 2021-04-06
 */
public enum TextRelationship implements Serializable {
    EQUAL,
    CONTAIN,
    START_WITH,
    END_WITH;

    public static boolean contains(String... names) {
        for (String name : names) {
            boolean flag = false;
            for (TextRelationship relationship : TextRelationship.values()) {
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
