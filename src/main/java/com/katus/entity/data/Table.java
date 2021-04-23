package com.katus.entity.data;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
@Getter
public class Table {
    private final Dataset<Row> dataset;
    private final Field[] fields;

    public Table(Dataset<Row> dataset, Field[] fields) {
        this.dataset = dataset;
        this.fields = fields;
    }
}
