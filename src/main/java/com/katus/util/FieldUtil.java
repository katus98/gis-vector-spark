package com.katus.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
public final class FieldUtil {
    public static String[] mergeFields(String[] fieldNames1, String[] fieldNames2) {
        String[] fieldNames = new String[fieldNames1.length + fieldNames2.length];
        int i = 0;
        for (String field : fieldNames1) {
            fieldNames[i++] = "target_" + field;
        }
        for (String field : fieldNames2) {
            fieldNames[i++] = "extent_" + field;
        }
        return fieldNames;
    }

    public static LinkedHashMap<String, Object> mergeAttributes(String[] fieldNames, Map<String, Object> attr1, Map<String, Object> attr2) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            if (fieldName.startsWith("target_")) {
                attributes.put(fieldName, attr1.getOrDefault(fieldName.substring(fieldName.indexOf("_") + 1), ""));
            } else {
                attributes.put(fieldName, attr2.getOrDefault(fieldName.substring(fieldName.indexOf("_") + 1), ""));
            }
        }
        return attributes;
    }
}
