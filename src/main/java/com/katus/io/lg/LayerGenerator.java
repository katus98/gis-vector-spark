package com.katus.io.lg;

import com.katus.entity.Layer;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-13
 */
@NoArgsConstructor
public abstract class LayerGenerator {
    protected SparkSession ss;

    protected LayerGenerator(SparkSession ss) {
        this.ss = ss;
    }

    public abstract Layer generate() throws Exception;
}
