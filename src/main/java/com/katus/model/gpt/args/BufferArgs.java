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
public class BufferArgs extends UnaryArgs {
    /**
     * 缓冲区距离
     */
    private String distance = "";
    /**
     * 缓冲区距离单位对应的坐标参考, 默认与输入数据地理参考一致
     */
    private String crs = "";

    public BufferArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !distance.isEmpty();
    }
}
