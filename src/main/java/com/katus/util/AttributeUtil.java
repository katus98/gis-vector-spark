package com.katus.util;

import com.katus.constant.FieldMark;
import com.katus.constant.FieldType;
import com.katus.constant.StatisticalMethod;
import com.katus.entity.data.Field;

import java.util.*;

/**
 * @author SUN Katus
 * @version 1.2, 2021-04-09
 */
public class AttributeUtil {
    /**
     * 属性字段值合并
     * @param fields 合并后的属性字段
     * @param attributesArray 属性字段值列表数组
     * @return 合并后的属性字段值列表
     */
    @SafeVarargs
    public static LinkedHashMap<Field, Object> merge(Field[] fields, Map<Field, Object>... attributesArray) {
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        List<Field> fieldList = Arrays.asList(fields);
        for (int i = 0; i < attributesArray.length; i++) {
            for (Map.Entry<Field, Object> entry : attributesArray[i].entrySet()) {
                Field field = entry.getKey();
                if (fieldList.contains(field)) {
                    attributes.put(fields[fieldList.indexOf(field)], entry.getValue());
                } else {
                    Field copyField = field.copy();
                    copyField.setName(field.getName() + "_" + i);
                    attributes.put(fields[fieldList.indexOf(copyField)], entry.getValue());
                }
            }
        }
        return attributes;
    }

    @Deprecated
    public static LinkedHashMap<Field, Object> mergeAttributes(Field[] fields, Map<Field, Object> attr1, Map<Field, Object> attr2) {
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        for (Field fieldName : fields) {
            Field oriField = fieldName.copy();
            oriField.setName(fieldName.getName().substring(fieldName.getName().indexOf("_") + 1));
            if (fieldName.getName().startsWith("target_")) {
                attributes.put(fieldName, attr1.getOrDefault(oriField, fieldName.getDefaultValue()));
            } else {
                attributes.put(fieldName, attr2.getOrDefault(oriField, fieldName.getDefaultValue()));
            }
        }
        return attributes;
    }

    public static LinkedHashMap<Field, Object> initStatistics(Field[] fields, Field[] categoryFields, Field[] summaryFields,
                                                              List<StatisticalMethod> statisticalMethods,
                                                              LinkedHashMap<Field, Object> oriAttr) {
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        int i = 0;
        for (Field categoryField : categoryFields) {
            attributes.put(fields[i++], oriAttr.get(categoryField));
        }
        for (Field summaryField : summaryFields) {
            for (StatisticalMethod statisticalMethod : statisticalMethods) {
                switch (statisticalMethod) {
                    case COUNT:
                        attributes.put(fields[i++], 1L);
                        break;
                    case MEAN:
                        attributes.put(fields[i++], 1.0);
                        break;
                    case SUM:
                        if (summaryField.getType().equals(FieldType.INTEGER)) {
                            attributes.put(fields[i++], Long.valueOf(oriAttr.get(summaryField).toString()));
                            break;
                        }
                    default:
                        attributes.put(fields[i++], oriAttr.get(summaryField));
                }
            }
        }
        return attributes;
    }

    public static LinkedHashMap<Field, Object> statistic(LinkedHashMap<Field, Object> attr1, LinkedHashMap<Field, Object> attr2) {
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        Iterator<Map.Entry<Field, Object>> it1 = attr1.entrySet().iterator();
        Iterator<Map.Entry<Field, Object>> it2 = attr2.entrySet().iterator();
        while (it1.hasNext() && it2.hasNext()) {
            Map.Entry<Field, Object> entry1 = it1.next();
            Map.Entry<Field, Object> entry2 = it2.next();
            Field field = entry1.getKey();
            if (!field.equals(entry2.getKey())) continue;
            FieldMark fieldMark = field.getMark();
            if (fieldMark.equals(FieldMark.ORIGIN)) {
                attributes.put(field, entry1.getValue());
            } else if (fieldMark.name().startsWith("STAT_")) {
                StatisticalMethod statisticalMethod = StatisticalMethod.getByFieldMark(fieldMark);
                Number result;
                switch (Objects.requireNonNull(statisticalMethod)) {
                    case SUM:
                        result = field.getType().equals(FieldType.INTEGER64) ?
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
                    default:
                        result = (Number) entry1.getValue();
                        break;
                }
                attributes.put(field, result);
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
