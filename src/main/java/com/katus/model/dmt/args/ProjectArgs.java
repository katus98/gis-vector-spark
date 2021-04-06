package com.katus.model.dmt.args;

import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 * @since 1.1
 */
@Getter
@Setter
public class ProjectArgs extends UnaryArgs {
    /**
     * 投影地理参考
     */
    private String crs = "";

    public ProjectArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !crs.isEmpty();
    }
}
