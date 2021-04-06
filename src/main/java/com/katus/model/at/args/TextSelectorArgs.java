package com.katus.model.at.args;

import com.katus.constant.TextRelationship;
import com.katus.model.base.args.UnaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2021-04-06
 */
@Getter
@Setter
public class TextSelectorArgs extends UnaryArgs {
    /**
     * 目标数据筛选字符串字段
     */
    private String selectField = "";
    /**
     * 用于筛选字符串的关系 (equal/contain/start_with/end_with)
     * @see TextRelationship
     */
    private String textRelationship = "equal";
    /**
     * 筛选关键字, ","分隔, 结果取并集
     */
    private String keywords = "";

    public TextSelectorArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && !selectField.isEmpty() && TextRelationship.contains(textRelationship) && !keywords.isEmpty();
    }
}
