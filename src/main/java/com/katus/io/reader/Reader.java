package com.katus.io.reader;

import com.katus.constant.GeometryType;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.data.Table;
import com.katus.entity.io.InputInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
@NoArgsConstructor
public abstract class Reader implements Serializable {
    protected SparkSession ss;
    protected InputInfo inputInfo;

    protected Reader(SparkSession ss, InputInfo inputInfo) {
        this.ss = ss;
        this.inputInfo = inputInfo;
    }

    public abstract Layer readToLayer();
    public abstract Table readToTable();
    public abstract boolean isValid();

    @Getter
    public abstract class ReaderHelper implements Serializable {
        protected String source;
        protected String name;
        protected String charset;
        protected Field[] fields;
        protected CoordinateReferenceSystem crs;
        protected GeometryType geometryType;

        protected ReaderHelper(String source) {
            this.source = source;
            this.name = inputInfo.getLayerNames().length > 0 ? inputInfo.getLayerNames()[0] : source;
        }

        protected void initAll() {
            initCharset();
            initReader();
            initFields();
            initCrs();
            initGeometryType();
        }

        protected abstract void initCharset();
        protected abstract void initReader();
        protected abstract void initFields();
        protected abstract void initCrs();
        protected abstract void initGeometryType();
    }
}
