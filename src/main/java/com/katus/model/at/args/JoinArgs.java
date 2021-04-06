package com.katus.model.at.args;

import com.katus.constant.JoinType;
import com.katus.model.base.args.BinaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class JoinArgs extends BinaryArgs {
    /**
     * 连接类型, 默认一对一连接 (one_to_one/one_to_many)
     * @see JoinType
     */
    private String joinType = "one_to_one";
    /**
     * 目标数据连接字段, ","分隔
     */
    private String joinFields1 = "";
    /**
     * 连接数据连接字段, ","分隔
     */
    private String joinFields2 = "";

    public JoinArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !joinFields1.isEmpty() && !joinFields2.isEmpty() && JoinType.contains(joinType);
    }
}
