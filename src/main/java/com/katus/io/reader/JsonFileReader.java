package com.katus.io.reader;

import com.katus.entity.data.Layer;
import com.katus.entity.data.Table;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import org.apache.spark.sql.SparkSession;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-14
 * @since 2.0
 */
// todo: finish the json file reader.
public class JsonFileReader extends Reader {

    protected JsonFileReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        return null;
    }

    @Override
    public Table readToTable() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        return null;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    public class JsonFileReaderHelper extends ReaderHelper {

        protected JsonFileReaderHelper(String source) {
            super(source);
        }

        @Override
        protected void initCharset() {

        }

        @Override
        protected void initReader() {

        }

        @Override
        protected void initFields() {

        }

        @Override
        protected void initCrs() {

        }

        @Override
        protected void initGeometryType() {

        }
    }
}
