package com.katus.model.dmt.args;

import com.katus.constant.JoinType;
import com.katus.constant.SpatialRelationship;
import com.katus.model.base.args.BinaryArgs;
import lombok.Getter;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 2.0, 2020-04-06
 */
@Getter
@Setter
public class SpatialJoinArgs extends BinaryArgs {
    /**
     * 连接类型, 默认一对一连接 (one_to_one/one_to_many)
     * @see JoinType
     */
    private String joinType = "one_to_one";
    /**
     * 空间关系类型, 默认相交
     * @see SpatialRelationship
     */
    private String spatialRelationship = "intersects";

    public SpatialJoinArgs(String[] args) {
        super(args);
    }

    @Override
    public Boolean isValid() {
        return super.isValid() && JoinType.contains(joinType) && SpatialRelationship.contains(spatialRelationship);
    }
}
