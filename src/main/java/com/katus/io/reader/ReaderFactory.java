package com.katus.io.reader;

import com.katus.constant.SourceType;
import com.katus.entity.io.Input;
import com.katus.entity.io.InputInfo;
import org.apache.spark.sql.SparkSession;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
public final class ReaderFactory {

    public static Reader create(SparkSession ss, InputInfo inputInfo) {
        Reader reader = null;
        switch (SourceType.getByInputInfo(inputInfo)) {
            case TEXT_FILE:
                reader = new TextFileReader(ss, inputInfo);
                break;
            case JSON_FILE:
                reader = new JsonFileReader(ss, inputInfo);
                break;
            case SHAPE_FILE:
                reader = new ShapeFileReader(ss, inputInfo);
                break;
            case GEO_DATABASE:
                reader = new GeoDatabaseReader(ss, inputInfo);
                break;
            case MYSQL_DATABASE:
                reader = new MySQLReader(ss, inputInfo);
                break;
            case POSTGRESQL_DATABASE:
                reader = new PostgreSQLReader(ss, inputInfo);
                break;
            case POSTGRESQL_DATABASE_CITUS:
                reader = new CitusPostgreSQLReader(ss, inputInfo);
        }
        return reader;
    }

    public static Reader create(SparkSession ss, Input input) {
        return create(ss, new InputInfo(input));
    }
}
