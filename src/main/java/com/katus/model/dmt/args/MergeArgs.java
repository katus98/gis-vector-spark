package com.katus.model.dmt.args;

import com.katus.model.base.args.BinaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class MergeArgs extends BinaryArgs {
    /**
     * 结果地理参考, 不填则与第一份输入数据相同
     */
    private String crs = "";

    public MergeArgs(String[] args) {
        super(args);
    }
}
