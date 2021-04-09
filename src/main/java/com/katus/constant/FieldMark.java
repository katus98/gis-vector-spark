package com.katus.constant;

import lombok.Getter;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-08
 * @since 2.0
 */
@Getter
public enum FieldMark implements Serializable {
    ORIGIN,
    STAT_COUNT,
    STAT_MAXIMUM,
    STAT_MINIMUM,
    STAT_SUM,
    STAT_MEAN;

    private final String prefix;
    private final String postfix;

    FieldMark() {
        String name = this.name();
        this.prefix = name.substring(name.indexOf("_") + 1) + "_";
        this.postfix = name.substring(name.indexOf("_"));
    }
}
