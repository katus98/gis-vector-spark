package com.katus.util;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-16
 */
public final class SparkUtil {
    private static final SparkSession ss;
    static {
        ss = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark Program")
                .getOrCreate();
    }

    public static SparkSession getSparkSession() {
        return ss;
    }

    public static SparkContext getSparkContext() {
        return ss.sparkContext();
    }

    public static JavaSparkContext getJavaSparkContext() {
        return JavaSparkContext.fromSparkContext(getSparkContext());
    }
}
