package com.katus.io.reader;

import com.katus.entity.data.Layer;
import com.katus.entity.data.Table;
import com.katus.entity.io.InputInfo;
import com.katus.exception.ParameterNotValidException;
import org.apache.spark.sql.SparkSession;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-13
 * @since 2.0
 */
public class MySQLReader extends RelationalDatabaseReader {

    protected MySQLReader(SparkSession ss, InputInfo inputInfo) {
        super(ss, inputInfo);
    }

    @Override
    public Layer readToLayer() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        MySQLReaderHelper readerHelper = this.new MySQLReaderHelper(inputInfo.getSource());
        return super.readToLayer(readerHelper, readMultipleTables(readerHelper));
    }

    @Override
    public Table readToTable() {
        if (!isValid()) {
            throw new ParameterNotValidException(inputInfo);
        }
        MySQLReaderHelper readerHelper = this.new MySQLReaderHelper(inputInfo.getSource());
        return new Table(readMultipleTables(readerHelper), readerHelper.fields);
    }

    public class MySQLReaderHelper extends RelationalDatabaseReaderHelper {
        private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

        protected MySQLReaderHelper(String source) {
            super(source, JDBC_DRIVER);
            super.initAll();
        }
    }
}
