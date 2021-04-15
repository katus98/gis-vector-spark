package com.katus.util;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-13
 * @since 2.0
 */
public final class ArrayUtil {

    public static boolean contains(int[] array, int element) {
        for (int i : array) {
            if (i == element) {
                return true;
            }
        } return false;
    }
}
