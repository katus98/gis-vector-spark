package com.katus.util;

import com.katus.constant.NumberType;
import com.katus.constant.StatisticalMethod;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-19
 */
public class AttributeUtil {
    public static LinkedHashMap<String, Object> merge(String[] fieldNames, Map<String, Object> attr1, Map<String, Object> attr2) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            if (attr1.containsKey(fieldName)) {
                attributes.put(fieldName, attr1.get(fieldName));
            } else if (attr2.containsKey(fieldName)) {
                attributes.put(fieldName, attr2.get(fieldName));
            } else {
                String oriField = fieldName.substring(0, fieldName.length() - 2);
                if (fieldName.endsWith("_1")) {
                    attributes.put(fieldName, attr1.getOrDefault(oriField, ""));
                } else if (fieldName.endsWith("_2")){
                    attributes.put(fieldName, attr2.getOrDefault(oriField, ""));
                } else {
                    attributes.put(fieldName, "");
                }
            }
        }
        return attributes;
    }

    @Deprecated
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

    public static LinkedHashMap<String, Object> initStatistics(LinkedHashMap<String, Object> oriAttr, String[] categoryFields, List<String> summaryFields, List<NumberType> numberTypes, List<StatisticalMethod> statisticalMethods) {
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
                        attributes.put(key + "(LONG)", 5L);
                        break;
                    case MAIN:
                        attributes.put(key + "(DOUBLE)", 1.0);
                        break;
                    default:
                        if (numberTypes.get(i).getIsDecimal()) {
                            attributes.put(key + "(DOUBLE)", Double.parseDouble(oriAttr.get(summaryFields.get(i)).toString()));
                        } else {
                            attributes.put(key + "(LONG)", Long.parseLong(oriAttr.get(summaryFields.get(i)).toString()));
                        }
                        break;
                }
            }
        }
        return attributes;
    }

    public static LinkedHashMap<String, Object> statistic(LinkedHashMap<String, Object> attr1, LinkedHashMap<String, Object> attr2, List<String> summaryFields) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        Iterator<Map.Entry<String, Object>> it1 = attr1.entrySet().iterator();
        Iterator<Map.Entry<String, Object>> it2 = attr2.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<String, Object> entry1 = it1.next();
            Map.Entry<String, Object> entry2 = it2.next();
            if (!entry1.getKey().contains("_")) {
                attributes.put(entry1.getKey(), entry1.getValue());
                continue;
            }
            if (summaryFields.contains(entry1.getKey().substring(0, entry1.getKey().lastIndexOf("_")))) {
                NumberType numberType = NumberType.getByFieldName(entry1.getKey());
                StatisticalMethod statisticalMethod = StatisticalMethod.getByFieldName(entry1.getKey());
                Number result;
                switch (statisticalMethod) {
                    case SUM:
                        result = numberType.equals(NumberType.LONG) ?
                                ((Number) entry1.getValue()).longValue() + ((Number) entry2.getValue()).longValue() :
                                ((Number) entry1.getValue()).doubleValue() + ((Number) entry2.getValue()).doubleValue();
                        break;
                    case COUNT:
                        result = ((Number) entry1.getValue()).longValue() + ((Number) entry2.getValue()).longValue();
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
