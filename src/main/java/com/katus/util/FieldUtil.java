package com.katus.util;

import com.katus.constant.FieldMark;
import com.katus.constant.FieldType;
import com.katus.constant.StatisticalMethod;
import com.katus.entity.data.Field;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author SUN Katus
 * @version 1.3, 2021-04-09
 */
@Slf4j
public final class FieldUtil {
    /**
     * 属性字段合并, 相同字段使用 "_index" 区分
     * @param fieldArrays 多个属性字段数组
     * @return 合并后的属性字段数组
     */
    public static Field[] merge(Field[]... fieldArrays) {
        if (fieldArrays.length == 0) return new Field[0];
        List<Field> fieldList = Arrays.asList(fieldArrays[0]);
        Set<Integer> indexSet = new HashSet<>();
        for (int i = 1; i < fieldArrays.length; i++) {
            for (Field field : fieldArrays[i]) {
                Field newField = field.copy();
                if (fieldList.contains(field)) {
                    indexSet.add(fieldList.indexOf(field));
                    newField.setName(field.getName() + "_" + i);
                }
                fieldList.add(newField);
            }
        }
        for (Integer index : indexSet) {
            Field firstSameField = fieldList.get(index);
            firstSameField.setName(firstSameField.getName() + "_0");
        }
        Field[] fields = new Field[fieldList.size()];
        fieldList.toArray(fields);
        return fields;
    }

    public static Field[] mergeToLeast(Field[]... fieldArrays) {
        if (fieldArrays.length == 0) return new Field[0];
        List<Field> fieldList = Arrays.stream(fieldArrays[0]).map(Field::copy).collect(Collectors.toList());
        for (int i = 1; i < fieldArrays.length; i++) {
            for (Field field : fieldArrays[i]) {
                if (!fieldList.contains(field)) fieldList.add(field.copy());
            }
        }
        Field[] fields = new Field[fieldList.size()];
        fieldList.toArray(fields);
        return fields;
    }

    @Deprecated
    public static Field[] mergeFields(Field[] fieldNames1, Field[] fieldNames2) {
        Field[] fieldNames = new Field[fieldNames1.length + fieldNames2.length];
        int i = 0;
        for (Field field : fieldNames1) {
            Field newField = field.copy();
            newField.setName("target_" + field.getName());
            fieldNames[i++] = newField;
        }
        for (Field field : fieldNames2) {
            Field newField = field.copy();
            newField.setName("extent_" + field.getName());
            fieldNames[i++] = newField;
        }
        return fieldNames;
    }

    public static Field[] initStatisticsFields(Field[] categoryFields, Field[] summaryFields, List<StatisticalMethod> statMethodList) {
        Field[] fields = new Field[categoryFields.length + summaryFields.length * statMethodList.size()];
        int i = 0;
        for (Field categoryField : categoryFields) {
            fields[i++] = categoryField;
        }
        for (Field summaryField : summaryFields) {
            for (StatisticalMethod statMethod : statMethodList) {
                Field statField = summaryField.copy();
                statField.setMark(statMethod.getFieldMark());
                switch (statMethod) {
                    case COUNT:
                        statField.setType(FieldType.INTEGER64);
                        break;
                    case MEAN:
                        statField.setType(FieldType.DECIMAL);
                        break;
                    case SUM:
                        if (summaryField.getType().equals(FieldType.INTEGER)) {
                            statField.setType(FieldType.INTEGER64);
                        }
                    default:
                        break;
                }
                fields[i++] = statField;
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

    public static Field getFieldByName(Field[] fields, String name) {
        for (Field field : fields) {
            if (field.getName().equals(name)) return field;
        }
        String msg = "Field " + name + " does not exist!";
        log.error(msg);
        throw new RuntimeException(msg);
    }

    public static Field getFieldByNameAndMark(Field[] fields, String name, FieldMark mark) {
        for (Field field : fields) {
            if (field.getName().equals(name) && field.getMark().equals(mark)) return field;
        }
        String msg = "Field " + name + " with " + mark.name() + " does not exist!";
        log.error(msg);
        throw new RuntimeException(msg);
    }
}
