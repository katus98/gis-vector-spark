package com.katus.constant;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.1, 2021-04-06
 */
public enum StatisticalMethod implements Serializable {
    COUNT,
    MAXIMUM,
    MINIMUM,
    SUM,
    MEAN;

    public String getFieldNamePostfix() {
        return "_#" + this.name() + "#";
    }

    public static StatisticalMethod getByFieldName(String fieldName) {
        return StatisticalMethod.valueOf(fieldName.substring(fieldName.lastIndexOf("_") + 2, fieldName.lastIndexOf("#")));
    }

    public static boolean contains(String... names) {
        for (String name : names) {
            boolean flag = false;
            for (StatisticalMethod method : StatisticalMethod.values()) {
                if (method.name().equals(name.toUpperCase())) {
                    flag = true;
                    break;
                }
            }
            if (!flag) return false;
        }
        return true;
    }
}
