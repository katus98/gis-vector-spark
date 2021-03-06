package com.katus.model.gpt.args;

import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2020-04-06
 */
@Getter
@Setter
public class ConvexHullArgs extends UnaryArgs {

    public ConvexHullArgs(String[] args) {
        super(args);
    }
}
