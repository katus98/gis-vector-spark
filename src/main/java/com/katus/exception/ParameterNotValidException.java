package com.katus.exception;

import com.katus.entity.io.InputInfo;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-12
 * @since 2.0
 */
public class ParameterNotValidException extends RuntimeException {

    public ParameterNotValidException(InputInfo inputInfo) {
        super("Parameters are not valid! Parameters: " + inputInfo.toString());
    }
}
