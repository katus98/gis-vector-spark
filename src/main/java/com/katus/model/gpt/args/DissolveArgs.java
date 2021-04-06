package com.katus.model.gpt.args;

import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class DissolveArgs extends UnaryArgs {
    /**
     * 目标数据溶解字段, ","分隔, 默认全部溶解
     */
    private String dissolveFields = "";

    public DissolveArgs(String[] args) {
        super(args);
    }
}
