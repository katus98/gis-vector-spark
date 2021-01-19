package com.katus.entity.args;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-13
 * @since 2.0
 */
@Getter
@Setter
@Slf4j
@ToString
public class Output {
    private String destination = "";
    private String header = "true";
    private String geometryFormat = "wkt";
    private String geometryField = "wkt";
    private String separator = "\t";
    private String charset = "UTF-8";
    private String crs = "4326";

    public Output(String[] args, String postfix) {
        for (int i = 0; i < args.length; i+=2) {
            if (args[i].startsWith("-output_") && args[i].endsWith(postfix)) {
                String fieldName = args[i].substring(8, args[i].length() - postfix.length());
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
        return !destination.isEmpty();
    }
}
