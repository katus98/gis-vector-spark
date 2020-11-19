package com.katus.util;

import com.katus.constant.NumberType;
import com.katus.constant.StatisticalMethod;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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

    public static String[] initStatisticsFields(String[] fieldNames, String[] categoryFields, List<String> summaryFields, List<StatisticalMethod> statisticalMethods) {
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
                fields[i++] = summaryFields.get(i) + statisticalMethod.getFieldNamePostfix();
            }
        }
        return fields;
    }

    public static LinkedHashMap<String, Object> initStatisticsAttributes(LinkedHashMap<String, Object> oriAttr, String[] categoryFields, List<String> summaryFields, List<NumberType> numberTypes, List<StatisticalMethod> statisticalMethods) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        if (!categoryFields[0].trim().isEmpty()) {
            for (String categoryField : categoryFields) {
                attributes.put(categoryField, oriAttr.get(categoryField));
            }
        }
        for (int i = 0; i < summaryFields.size(); i++) {
            for (StatisticalMethod statisticalMethod : statisticalMethods) {
                String key = summaryFields.get(i) + statisticalMethod.getFieldNamePostfix();
                switch (statisticalMethod) {
                    case COUNT:
                        attributes.put(key + "(LONG)", 1L);
                        break;
                    case MAIN:
                        attributes.put(key + "(DOUBLE)", 1.0);
                        break;
                    default:
                        if (numberTypes.get(i).getIsDecimal()) {
                            attributes.put(key + "(DOUBLE)", Double.parseDouble((String) oriAttr.get(summaryFields.get(i))));
                        } else {
                            attributes.put(key + "(LONG)", Long.parseLong((String) oriAttr.get(summaryFields.get(i))));
                        }
                        break;
                }
            }
        }
        return attributes;
    }

    public static LinkedHashMap<String, Object> statisticAttributes(LinkedHashMap<String, Object> attr1, LinkedHashMap<String, Object> attr2, List<String> summaryFields) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        Iterator<Map.Entry<String, Object>> it1 = attr1.entrySet().iterator();
        Iterator<Map.Entry<String, Object>> it2 = attr2.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<String, Object> entry1 = it1.next();
            Map.Entry<String, Object> entry2 = it2.next();
            if (summaryFields.contains(entry1.getKey().substring(0, entry1.getKey().lastIndexOf("_")))) {
                NumberType numberType = NumberType.getByFieldName(entry1.getKey());
                StatisticalMethod statisticalMethod = StatisticalMethod.getByFieldName(entry1.getKey());
                Number result;
                switch (statisticalMethod) {
                    case SUM:
                        result = numberType.equals(NumberType.LONG) ?
                                ((Long) entry1.getValue()) + ((Long) entry2.getValue()) :
                                ((Double) entry1.getValue()) + ((Double) entry2.getValue());
                        break;
                    case COUNT:
                        result = ((Long) entry1.getValue()) + ((Long) entry2.getValue());
                        break;
                    case MAXIMUM:
                        result = max((Number) entry1.getValue(), (Number) entry2.getValue());
                        break;
                    case MINIMUM:
                        result = min((Number) entry1.getValue(), (Number) entry2.getValue());
                        break;
                    case MAIN:
                    default:
                        result = (Number) entry1.getValue();
                        break;
                }
                attributes.put(entry1.getKey(), result);
            } else {
                attributes.put(entry1.getKey(), entry1.getValue());
            }
        }
        return attributes;
    }

    private static Number max(Number number1, Number number2) {
        return number1.doubleValue() > number2.doubleValue() ? number1 : number2;
    }

    private static Number min(Number number1, Number number2) {
        return number1.doubleValue() < number2.doubleValue() ? number1 : number2;
    }
}
