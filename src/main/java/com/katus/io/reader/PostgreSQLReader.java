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
public class PostgreSQLReader extends RelationalDatabaseReader {

    protected PostgreSQLReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        PostgreSQLReaderHelper readerHelper = this.new PostgreSQLReaderHelper(inputInfo.getSource());
        return super.readToLayer(readerHelper, readMultipleTables(readerHelper));
    }

    @Override
    public Table readToTable() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        PostgreSQLReaderHelper readerHelper = this.new PostgreSQLReaderHelper(inputInfo.getSource());
        return new Table(readMultipleTables(readerHelper), readerHelper.fields);
    }

    public class PostgreSQLReaderHelper extends RelationalDatabaseReaderHelper {
        private static final String JDBC_DRIVER = "org.postgresql.Driver";

        protected PostgreSQLReaderHelper(String source) {
            super(source, JDBC_DRIVER);
            super.initAll();
        }
    }
}
