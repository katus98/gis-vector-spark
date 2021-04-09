package com.katus.constant;

import lombok.Getter;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.2, 2021-04-08
 */
@Getter
public enum StatisticalMethod implements Serializable {
    COUNT(FieldMark.STAT_COUNT),
    MAXIMUM(FieldMark.STAT_MAXIMUM),
    MINIMUM(FieldMark.STAT_MINIMUM),
    SUM(FieldMark.STAT_SUM),
    MEAN(FieldMark.STAT_MEAN);

    private final FieldMark fieldMark;

    StatisticalMethod(FieldMark fieldMark) {
        this.fieldMark = fieldMark;
    }

    public static StatisticalMethod getByFieldMark(FieldMark fieldMark) {
        String name = fieldMark.name();
        if (name.startsWith("STAT_")) {
            return StatisticalMethod.valueOf(name.substring(5));
        } else return null;
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
