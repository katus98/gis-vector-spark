package com.katus.model.gpt.args;

import com.katus.model.base.args.BinaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class UnionArgs extends BinaryArgs {

    public UnionArgs(String[] args) {
        super(args);
    }
}
