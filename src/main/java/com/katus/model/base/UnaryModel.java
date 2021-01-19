package com.katus.model.base;

import com.katus.entity.Layer;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-13
 * @since 2.0
 */
public abstract class UnaryModel extends Model {
    public abstract Layer run(Layer layer);
}
