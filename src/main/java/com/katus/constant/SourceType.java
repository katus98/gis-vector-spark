package com.katus.constant;

import com.katus.entity.io.InputInfo;
import com.katus.exception.SourceTypeNotSupportException;

import java.util.Objects;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
public enum SourceType {
    TEXT_FILE,
    JSON_FILE,
    SHAPE_FILE,
    GEO_DATABASE,
    MYSQL_DATABASE,
    POSTGRESQL_DATABASE,
    POSTGRESQL_DATABASE_CITUS;

    public static SourceType getByInputInfo(InputInfo inputInfo) {
        if (Objects.isNull(inputInfo.getSource())) {
            throw new SourceTypeNotSupportException("null");
        }
        String source = inputInfo.getSource().toLowerCase();
        if (source.startsWith("jdbc:")) {
            if (source.contains(":mysql:")) {
                return MYSQL_DATABASE;
            } else if (source.contains(":postgresql:")) {
                if (!inputInfo.getSerialField().isEmpty()) {
                    return POSTGRESQL_DATABASE_CITUS;
                } else {
                    return POSTGRESQL_DATABASE;
                }
            } else {
                throw new SourceTypeNotSupportException(inputInfo.getSource());
            }
        } else if (source.startsWith("file://") || source.startsWith("hdfs://")) {
            if (source.endsWith(".json")) {
                return JSON_FILE;
            } else if (source.endsWith(".shp")) {
                return SHAPE_FILE;
            } else if (source.endsWith(".gdb") || source.endsWith(".mdb")) {
                return GEO_DATABASE;
            } else {
                return TEXT_FILE;
            }
        } else {
            throw new SourceTypeNotSupportException(inputInfo.getSource());
        }
    }
}
