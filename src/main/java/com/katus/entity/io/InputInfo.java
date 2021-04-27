package com.katus.entity.io;

import com.katus.constant.GeometryFieldFormat;
import com.katus.constant.GeometryType;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-09
 * @since 2.0
 */
@Getter
public class InputInfo implements Serializable {
    private final String source;
    private final String[] layerNames;
    private final Boolean header;
    private final GeometryFieldFormat geometryFieldFormat;
    private final String[] geometryFieldNames;
    private final GeometryType geometryType;
    private final String separator;
    private final String charset;
    private final String crs;
    private final String username;
    private final String password;
    private final String serialField;

    private InputInfo(InputInfoBuilder builder) {
        this.source = builder.source;
        this.layerNames = builder.layerNames;
        this.header = builder.header;
        this.geometryFieldFormat = builder.geometryFieldFormat;
        this.geometryFieldNames = builder.geometryFieldNames;
        this.geometryType = builder.geometryType;
        this.separator = builder.separator;
        this.charset = builder.charset;
        this.crs = builder.crs;
        this.username = builder.username;
        this.password = builder.password;
        this.serialField = builder.serialField;
    }

    public InputInfo(Input input) {
        this.source = input.getSource();
        this.layerNames = Arrays
                .stream(input.getLayerNames().split(","))
                .filter(str -> !str.isEmpty())
                .toArray(String[]::new);
        this.header = Boolean.valueOf(input.getHeader());
        this.geometryFieldFormat = GeometryFieldFormat.valueOf(input.getGeometryFieldFormat().toUpperCase());
        this.geometryFieldNames = Arrays
                .stream(input.getGeometryFieldNames().split(","))
                .filter(str -> !str.isEmpty())
                .toArray(String[]::new);
        this.geometryType = GeometryType.valueOf(input.getGeometryType().toUpperCase());
        this.separator = input.getSeparator();
        this.charset = input.getCharset();
        this.crs = input.getCrs();
        this.username = input.getUsername();
        this.password = input.getPassword();
        this.serialField = input.getSerialField();
    }

    @Override
    public String toString() {
        return "InputInfo{" +
                "source='" + source + '\'' +
                ", layerNames=" + Arrays.toString(layerNames) +
                ", header=" + header +
                ", geometryFieldFormat=" + geometryFieldFormat +
                ", geometryFieldNames=" + Arrays.toString(geometryFieldNames) +
                ", geometryType=" + geometryType +
                ", separator='" + separator + '\'' +
                ", charset='" + charset + '\'' +
                ", crs='" + crs + '\'' +
                ", serialField='" + serialField + '\'' +
                '}';
    }

    public static class InputInfoBuilder {
        private final String source;
        private String[] layerNames = InputInfoDefault.LAYER_NAMES;
        private Boolean header = InputInfoDefault.HEADER;
        private GeometryFieldFormat geometryFieldFormat = InputInfoDefault.GEOMETRY_FIELD_FORMAT;
        private String[] geometryFieldNames = InputInfoDefault.GEOMETRY_FIELD_NAMES;
        private GeometryType geometryType = InputInfoDefault.GEOMETRY_TYPE;
        private String separator = InputInfoDefault.SEPARATOR;
        private String charset = InputInfoDefault.CHARSET;
        private String crs = InputInfoDefault.CRS;
        private String username = InputInfoDefault.USERNAME;
        private String password = InputInfoDefault.PASSWORD;
        private String serialField = InputInfoDefault.SERIAL_FIELD;

        public InputInfoBuilder(@NonNull String source) {
            this.source = source;
        }

        public InputInfoBuilder layerNames(String[] layerNames) {
            this.layerNames = layerNames;
            return this;
        }

        public InputInfoBuilder header(boolean header) {
            this.header = header;
            return this;
        }

        public InputInfoBuilder geometryFieldFormat(GeometryFieldFormat format) {
            this.geometryFieldFormat = format;
            return this;
        }

        public InputInfoBuilder geometryFieldNames(String[] geometryFieldNames) {
            this.geometryFieldNames = geometryFieldNames;
            return this;
        }

        public InputInfoBuilder geometryType(GeometryType type) {
            this.geometryType = type;
            return this;
        }

        public InputInfoBuilder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public InputInfoBuilder charset(String charset) {
            this.charset = charset;
            return this;
        }

        public InputInfoBuilder crs(String crs) {
            this.crs = crs;
            return this;
        }

        public InputInfoBuilder username(String username) {
            this.username = username;
            return this;
        }

        public InputInfoBuilder password(String password) {
            this.password = password;
            return this;
        }

        public InputInfoBuilder serialField(String serialField) {
            this.serialField = serialField;
            return this;
        }

        public InputInfo build() {
            return new InputInfo(this);
        }

        public final static class InputInfoDefault {
            public static String[] LAYER_NAMES = new String[0];
            public static Boolean HEADER = true;
            public static GeometryFieldFormat GEOMETRY_FIELD_FORMAT = GeometryFieldFormat.WKT;
            public static String[] GEOMETRY_FIELD_NAMES = new String[]{"wkt"};
            public static GeometryType GEOMETRY_TYPE = GeometryType.POLYGON;
            public static String SEPARATOR = "\t";
            public static String CHARSET = "UTF-8";
            public static String CRS = "4326";
            public static String USERNAME = "root";
            public static String PASSWORD = "";
            public static String SERIAL_FIELD = "";
        }
    }
}
