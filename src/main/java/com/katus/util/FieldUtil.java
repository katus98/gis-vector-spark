package com.katus.util;

import com.katus.constant.StatisticalMethod;

import java.util.Arrays;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-17
 */
public final class FieldUtil {
    public static String[] merge(String[] fieldNames1, String[] fieldNames2) {
        List<String> fields1 = Arrays.asList(fieldNames1);
        List<String> fields2 = Arrays.asList(fieldNames2);
        String[] fieldNames = new String[fieldNames1.length + fieldNames2.length];
        int i = 0;
        for (String field : fields1) {
            fieldNames[i++] = fields2.contains(field) ? field + "_1" : field;
        }
        for (String field : fields2) {
            fieldNames[i++] = fields1.contains(field) ? field + "_2" : field;
        }
        return fieldNames;
    }

    @Deprecated
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

    public static String[] initStatisticsFields(String[] categoryFields, List<String> summaryFields, List<StatisticalMethod> statisticalMethods) {
        String[] fields;
        int i = 0;
        if (categoryFields[0].trim().isEmpty()) {
            fields = new String[summaryFields.size() * statisticalMethods.size()];
        } else {
            fields = new String[categoryFields.length + summaryFields.size() * statisticalMethods.size()];
            for (String categoryField : categoryFields) {
                fields[i++] = categoryField;
            }
        }
        for (String summaryField : summaryFields) {
            for (StatisticalMethod statisticalMethod : statisticalMethods) {
                fields[i++] = summaryField + statisticalMethod.getFieldNamePostfix();
            }
        }
        return fields;
    }

    public static int indexOfGeomField(String fieldName, String[] geometryFields) {
        int index = -1;
        for (int i = 0; i < geometryFields.length; i++) {
            if (geometryFields[i].equals(fieldName)) {
                index = i;
                break;
            }
        }
        return index;
    }

    public static String[] excludeGeomFields(String[] allFields, String[] geomFields) {
        String[] fields = new String[allFields.length - geomFields.length];
        for (int i = 0, j = 0; i < allFields.length; i++) {
            if (FieldUtil.indexOfGeomField(allFields[i], geomFields) == -1) {
                fields[j++] = allFields[i];
            }
        }
        return fields;
    }
}
