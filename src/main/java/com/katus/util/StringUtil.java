package com.katus.util;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-13
 * @since 2.0
 */
public final class StringUtil {

    public static boolean hasMoreThanOne(String... strArray) {
        if (strArray == null || strArray.length == 0) {
            return false;
        }
        boolean result = true;
        for (String str : strArray) {
            result &= str != null && !str.isEmpty();
        }
        return result;
    }

    public static boolean allNotNull(Object... objArray) {
        if (objArray == null) {
            return false;
        }
        boolean result = true;
        for (Object object : objArray) {
            result &= object != null;
        }
        return result;
    }
}
