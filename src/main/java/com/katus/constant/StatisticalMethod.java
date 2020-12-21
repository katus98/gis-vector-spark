package com.katus.constant;

import lombok.Getter;

import java.io.Serializable;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-19
 */
public enum StatisticalMethod implements Serializable {
    COUNT("count"),
    MAXIMUM("max"),
    MINIMUM("min"),
    SUM("sum"),
    MEAN("avg");

    @Getter
    private final String funName;

    StatisticalMethod(String funName) {
        this.funName = funName;
    }

    public String getFieldNamePostfix() {
        return "_#" + this.name() + "#";
    }

    public static StatisticalMethod getByFieldName(String fieldName) {
        return StatisticalMethod.valueOf(fieldName.substring(fieldName.lastIndexOf("_") + 2, fieldName.lastIndexOf("#")));
    }
}
