package com.katus.entity.data;

import com.katus.constant.FieldMark;
import com.katus.constant.FieldType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.Date;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-20
 * @since 2.0
 */
@AllArgsConstructor
@Getter
@Setter
public class Field implements Serializable {
    private String name;   // 字段名称
    private String alias;   // 字段别名
    private FieldType type;   // 字段类型
    private String description;   // 字段描述
    private FieldMark mark;   // 字段标记

    public Field(String name) {
        this.name = name;
        this.alias = name;
        this.type = FieldType.TEXT;
        this.description = "";
        this.mark = FieldMark.ORIGIN;
    }

    public Field(String name, Class<?> clazz) {
        this.name = name;
        this.alias = name;
        this.description = "";
        this.mark = FieldMark.ORIGIN;
        if (clazz.equals(Integer.class) || clazz.equals(Short.class)) {
            this.type = FieldType.INTEGER;
        } else if (clazz.equals(Long.class)) {
            this.type = FieldType.INTEGER64;
        } else if (clazz.equals(Double.class) || clazz.equals(Float.class)) {
            this.type = FieldType.DECIMAL;
        } else if (clazz.equals(Date.class)) {
            this.type = FieldType.DATE;
        } else {
            this.type = FieldType.TEXT;
        }
    }

    public Field(String name, DataType dataType) {
        this(name);
        // todo: find the banding between SparkSQL datatype and Java datatype.
    }

    public Field(String name, int geoDbTypeId) {
        this(name);
        // todo: find the banding between geoDB type and Java datatype.
    }

    private Field(Field field) {
        this.name = field.getName();
        this.alias = field.getAlias();
        this.type = field.getType();
        this.description = field.getDescription();
        this.mark = field.getMark();
    }

    @Override
    public int hashCode() {
        return name.hashCode() + type.hashCode() + mark.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Field) {
            Field other = (Field) obj;
            return name != null && name.equals(other.getName())
                    && type != null && type.equals(other.getType())
                    && mark != null && mark.equals(other.getMark());
        }
        return false;
    }

    public boolean allEquals(Object obj) {
        if (this.equals(obj)) {
            Field other = (Field) obj;
            return alias != null && alias.equals(other.getAlias())
                    && description != null && description.equals(other.getDescription());
        }
        return false;
    }

    @Override
    public String toString() {
        return name + mark.getPostfix();
    }

    public Field copy() {
        Field field;
        try {
            field = (Field) this.clone();
        } catch (CloneNotSupportedException e) {
            field = new Field(this);
        }
        return field;
    }

    public Field copy(FieldMark mark) {
        Field field = this.copy();
        field.setMark(mark);
        return field;
    }

    public Object getDefaultValue() {
        return type.getDefaultVale();
    }
}
