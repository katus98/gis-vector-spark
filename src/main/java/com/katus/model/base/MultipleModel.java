package com.katus.model.base;

import com.katus.entity.data.Layer;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-06
 */
public abstract class MultipleModel extends Model {
    public abstract Layer run(Layer... layers);
}
