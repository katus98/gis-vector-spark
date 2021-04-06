package com.katus.entity.io;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-13
 * @since 2.0
 */
@Data
@Slf4j
public class Input {
    private String source = "";
    private String header = "true";
    private String geometryFormat = "wkt";
    private String geometryFields = "wkt";
    private String geometryType = "LineString";
    private String separator = "\t";
    private String charset = "UTF-8";
    private String crs = "4326";

    public Input(String[] args) {
        this(args, "");
    }

    public Input(String[] args, String postfix) {
        for (int i = 0; i < args.length; i+=2) {
            if (args[i].startsWith("-input_") && args[i].endsWith(postfix)) {
                String fieldName = args[i].substring(7, args[i].length() - postfix.length());
                try {
                    Field field = this.getClass().getDeclaredField(fieldName);
                    field.setAccessible(true);
                    field.set(this, args[i+1]);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    log.debug(fieldName + " assignment error!");
                }
            }
        }
    }

    public Boolean isValid() {
        return !source.isEmpty();
    }
}
