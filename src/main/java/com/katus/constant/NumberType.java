package com.katus.constant;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.2, 2021-04-09
 */
@Deprecated
public enum NumberType implements Serializable {
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    @Deprecated
    BIG_INTEGER,
    @Deprecated
    BIG_DECIMAL;

    public String getClassFullName() {
        switch (this) {
            case SHORT:
                return "java.lang.Short";
            case INTEGER:
                return "java.lang.Integer";
            case LONG:
                return "java.lang.Long";
            case FLOAT:
                return "java.lang.Float";
            case DOUBLE:
                return "java.lang.Double";
            /*
            case BIG_INTEGER:
                return "java.math.BigInteger";
            case BIG_DECIMAL:
                return "java.math.BigDecimal";
            */
            default:
                return null;
        }
    }

    public boolean getIsDecimal() {
        return this.equals(FLOAT) || this.equals(DOUBLE);
    }

    public static NumberType getByFieldName(String fieldName) {
        return NumberType.valueOf(fieldName.substring(fieldName.lastIndexOf("(") + 1, fieldName.lastIndexOf(")")).toUpperCase());
    }

    public static boolean contains(String... names) {
        for (String name : names) {
            boolean flag = false;
            for (NumberType type : NumberType.values()) {
                if (type.name().equals(name.toUpperCase())) {
                    flag = true;
                    break;
                }
            }
            if (!flag) return false;
        }
        return true;
    }
}
