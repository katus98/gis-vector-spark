package com.katus.model.args;

import com.katus.constant.JoinType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author Sun Katus
 * @version 1.2, 2020-12-08
 */
@Getter
@Setter
@Slf4j
public class FieldJoinArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // true, false
    /**
     * @see JoinType
     */
    @Option(name = "-joinType", usage = "连接类型")
    private String joinType = "one_to_one";   // one_to_one, one_to_many

    @Option(name = "-input1", usage = "输入目标数据路径", required = true)
    private String input1;

    @Option(name = "-joinFields1", usage = "输入目标数据连接字段", required = true)
    private String joinFields1;   // separate by ","
    /**
     * The below is only for specific inputs, not always takes effect.
     */
    @Option(name = "-hasHeader1", usage = "输入目标数据是否含有标题行")
    private String hasHeader1 = "true";   // true, false

    @Option(name = "-isWkt1", usage = "输入目标数据几何列是否是WKT")
    private String isWkt1 = "true";   // true, false

    @Option(name = "-geometryFields1", usage = "输入目标数据几何列")
    private String geometryFields1 = "wkt";   // separate by ","

    @Option(name = "-geometryType1", usage = "输入目标数据几何类型")
    private String geometryType1 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator1", usage = "输入目标数据分隔符")
    private String separator1 = "\t";

    @Option(name = "-crs1", usage = "输入目标数据地理参考")
    private String crs1 = "4490";

    @Option(name = "-charset1", usage = "输入目标数据字符集")
    private String charset1 = "UTF-8";   // UTF-8, GBK, US-ASCII, ISO-8859-1, UTF-16BE, UTF-16LE, UTF-16

    @Option(name = "-serialField1", usage = "输入目标数据顺序自增字段")
    private String serialField1 = "_id";

    @Option(name = "-input2", usage = "输入范围数据路径", required = true)
    private String input2;

    @Option(name = "-joinFields2", usage = "输入范围数据连接字段", required = true)
    private String joinFields2;   // separate by ","
    /**
     * The below is only for specific inputs, not always takes effect.
     */
    @Option(name = "-hasHeader2", usage = "输入范围数据是否含有标题行")
    private String hasHeader2 = "true";   // true, false

    @Option(name = "-isWkt2", usage = "输入范围数据几何列是否是WKT")
    private String isWkt2 = "true";   // true, false

    @Option(name = "-geometryFields2", usage = "输入范围数据几何列")
    private String geometryFields2 = "wkt";   // separate by ","

    @Option(name = "-geometryType2", usage = "输入范围数据几何类型")
    private String geometryType2 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator2", usage = "输入范围数据分隔符")
    private String separator2 = "\t";

    @Option(name = "-crs2", usage = "输入范围数据地理参考")
    private String crs2 = "4490";

    @Option(name = "-charset2", usage = "输入范围数据字符集")
    private String charset2 = "UTF-8";   // UTF-8, GBK, US-ASCII, ISO-8859-1, UTF-16BE, UTF-16LE, UTF-16

    @Option(name = "-serialField2", usage = "输入范围数据顺序自增字段")
    private String serialField2 = "_id";

    public static FieldJoinArgs initArgs(String[] args) {
        FieldJoinArgs mArgs = new FieldJoinArgs();
        CmdLineParser parser = new CmdLineParser(mArgs);
        try {
            parser.parseArgument(args);
            return mArgs;
        } catch (CmdLineException e) {
            log.error(e.getLocalizedMessage());
            return null;
        }
    }
}
