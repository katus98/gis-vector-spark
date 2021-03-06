package com.katus.model.base.args;

import com.katus.entity.io.Input;
import com.katus.entity.io.Output;

import java.util.Map;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-13
 * @since 2.0
 */
public interface ArgsAble {
    Input[] getInputs();
    Output[] getOutputs();
    Map<String, Object> getSpecialArgs();
    Boolean isValid();
}
