package com.katus.model.rt.args;

import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class RandomSelectionArgs extends UnaryArgs {
    /**
     * 数据抽样比例 (0-1)
     */
    private String ratio = "";

    public RandomSelectionArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !ratio.isEmpty();
    }
}
