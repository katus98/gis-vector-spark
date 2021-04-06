package com.katus.model.gt.args;

import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class MultiToSingleArgs extends UnaryArgs {

    public MultiToSingleArgs(String[] args) {
        super(args);
    }
}
