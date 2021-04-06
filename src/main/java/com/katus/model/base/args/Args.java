package com.katus.model.base.args;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
@Slf4j
public abstract class Args implements ArgsAble {
    protected Args(String[] args) {
        for (int i = 0; i < args.length; i+=2) {
            if (!args[i].startsWith("-input_") && !args[i].startsWith("-output_")) {
                String fieldName = args[i].substring(1);
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

    @Override
    public Map<String, Object> getSpecialArgs() {
        Map<String, Object> argsMap = new HashMap<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            try {
                argsMap.put(field.getName(), field.get(this));
            } catch (IllegalAccessException e) {
                argsMap.put(field.getName(), "");
                log.error(field.getName() + " get args error!");
            }
        }
        return argsMap;
    }
}
