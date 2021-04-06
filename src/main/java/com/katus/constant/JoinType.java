package com.katus.constant;

/**
 * 数据连接类型
 * @author SUN Katus
 * @version 1.1, 2021-04-06
 */
public enum JoinType {
    ONE_TO_ONE,   // 一对一连接
    ONE_TO_MANY;   // 一对多连接

    public static boolean contains(String name) {
        for (JoinType type : JoinType.values()) {
            if (type.name().equals(name.toUpperCase())) return true;
        }
        return false;
    }
}
