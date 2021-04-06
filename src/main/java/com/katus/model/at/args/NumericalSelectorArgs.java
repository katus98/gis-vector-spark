package com.katus.model.at.args;

import com.katus.constant.NumberRelationship;
import com.katus.constant.NumberType;
import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class NumericalSelectorArgs extends UnaryArgs {
    /**
     * 目标数据筛选数值字段
     */
    private String selectField = "";
    /**
     * 用于筛选的数值关系 (大小关系)
     * @see NumberRelationship
     */
    private String numberRelationship = "==";
    /**
     * 数值字段类型
     * @see NumberType
     */
    private String numberType = "Double";
    /**
     * 筛选阈值
     */
    private String threshold = "";

    public NumericalSelectorArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !selectField.isEmpty() && NumberRelationship.getBySymbol(numberRelationship) != null
                && NumberType.contains(numberType) && !threshold.isEmpty();
    }
}
