package com.katus.model.at.args;

import com.katus.constant.NumberType;
import com.katus.constant.StatisticalMethod;
import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class StatisticsArgs extends UnaryArgs {
    /**
     * 目标数据分类字段, ","分隔, 如果为空则结果只有一行统计信息
     */
    private String categoryFields = "";
    /**
     * 目标数据汇总数值字段, ","分隔
     */
    private String summaryFields = "";
    /**
     * 汇总数值字段数据类型, ","分隔
     * @see NumberType
     */
    private String numberTypes = "";
    /**
     * 汇总统计方法, ","分隔
     * @see StatisticalMethod
     */
    private String statisticalMethods = "";

    public StatisticsArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !summaryFields.isEmpty() && NumberType.contains(numberTypes.split(","))
                && StatisticalMethod.contains(statisticalMethods.split(","));
    }
}
