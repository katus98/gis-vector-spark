package com.katus.constant;

import lombok.Getter;

import java.util.Date;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-20
 * @since 2.0
 */
@Getter
public enum FieldType {
    INTEGER(Integer.class, 0),   // 整型或短整型
    INTEGER64(Long.class, 0L),   // 长整型
    DECIMAL(Double.class, 0.0),   // 浮点数
    TEXT(String.class, ""),   // 字符串 (默认)
    DATE(Date.class, new Date());   // 日期

    private final Class<?> clazz;
    private final Object defaultVale;

    FieldType(Class<?> clazz, Object defaultVale) {
        this.clazz = clazz;
        this.defaultVale = defaultVale;
    }
}
