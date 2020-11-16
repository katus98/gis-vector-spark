package com.katus.io.writer;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-13
 */
public class TextFileOutputFormat extends MultipleTextOutputFormat<String, String> {
    public static String textFilename = "result.tsv";

    @Override
    protected String generateFileNameForKeyValue(String key, String value, String name) {
        return name;
    }

    @Override
    protected String generateActualValue(String key, String value) {
        return value;
    }
}
