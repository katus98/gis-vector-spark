package com.katus.constant;

import com.katus.util.InputUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-11
 * @since 1.2
 */
public final class ProvinceExtent {
    private final static Properties EXTENTS;
    static {
        EXTENTS = new Properties();
        InputStream is = InputUtil.class.getResourceAsStream("/provinces.properties");
        try {
            EXTENTS.load(is);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Provinces' extent properties loads failed.");
        }
    }

    public static Double[] getExtentByName(String name) {
        String extentStr = EXTENTS.getProperty(name);
        return Arrays.stream(extentStr.split(",")).map(Double::parseDouble).toArray(Double[]::new);
    }
}
