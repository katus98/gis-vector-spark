package com.katus.model.args;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author Keran Sun (katus)
 * @version 2.0, 2020-11-19
 */
@Getter
@Setter
@Slf4j
public class IntersectionArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // false, true

    @Option(name = "-crs", usage = "运算过程的地理参考")
    private String crs = "4326";   // 4326, 3857

    @Option(name = "-input1", usage = "输入目标数据路径", required = true)
    private String input1;
    /**
     * The below is only for text file
     */
    @Option(name = "-hasHeader1", usage = "输入目标数据是否含有标题行")
    private String hasHeader1 = "false";   // false, true

    @Option(name = "-isWkt1", usage = "输入目标数据几何列是否是WKT")
    private String isWkt1 = "true";   // true, false

    @Option(name = "-geometryFields1", usage = "输入目标数据几何列")
    private String geometryFields1 = "-1";   // separate by ","

    @Option(name = "-geometryType1", usage = "输入目标数据几何类型")
    private String geometryType1 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator1", usage = "输入目标数据分隔符")
    private String separator1 = "\t";

    @Option(name = "-crs1", usage = "输入目标数据地理参考")
    private String crs1 = "4326";   // 4326, 3857

    @Option(name = "-charset1", usage = "输入目标数据字符集")
    private String charset1 = "UTF-8";   // UTF-8, GBK

    @Option(name = "-input2", usage = "输入范围数据路径", required = true)
    private String input2;
    /**
     * The below is only for text file
     */
    @Option(name = "-hasHeader2", usage = "输入范围数据是否含有标题行")
    private String hasHeader2 = "false";   // false, true

    @Option(name = "-isWkt2", usage = "输入范围数据几何列是否是WKT")
    private String isWkt2 = "true";   // true, false

    @Option(name = "-geometryFields2", usage = "输入范围数据几何列")
    private String geometryFields2 = "-1";   // separate by ","

    @Option(name = "-geometryType2", usage = "输入目标数据几何类型")
    private String geometryType2 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator2", usage = "输入范围数据分隔符")
    private String separator2 = "\t";

    @Option(name = "-crs2", usage = "输入范围数据地理参考")
    private String crs2 = "4326";   // 4326, 3857

    @Option(name = "-charset2", usage = "输入范围数据字符集")
    private String charset2 = "UTF-8";   // UTF-8, GBK

    public static IntersectionArgs initArgs(String[] args) {
        IntersectionArgs mArgs = new IntersectionArgs();
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
