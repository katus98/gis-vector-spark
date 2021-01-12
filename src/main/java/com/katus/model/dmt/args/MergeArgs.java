package com.katus.model.dmt.args;

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
public class MergeArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // true, false

    @Option(name = "-crs", usage = "运算过程与结果的地理参考")
    private String crs = "4326";

    @Option(name = "-input1", usage = "输入数据1路径", required = true)
    private String input1;

    @Option(name = "-layers1", usage = "输入数据1图层名称")
    private String layers1 = "";
    /**
     * The below is only for specific inputs, not always takes effect.
     */
    @Option(name = "-hasHeader1", usage = "输入数据1是否含有标题行")
    private String hasHeader1 = "true";   // true, false

    @Option(name = "-isWkt1", usage = "输入数据1几何列是否是WKT")
    private String isWkt1 = "true";   // true, false

    @Option(name = "-geometryFields1", usage = "输入数据1几何列")
    private String geometryFields1 = "wkt";   // separate by ","

    @Option(name = "-geometryType1", usage = "输入数据1几何类型")
    private String geometryType1 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator1", usage = "输入数据1分隔符")
    private String separator1 = "\t";

    @Option(name = "-crs1", usage = "输入数据1地理参考")
    private String crs1 = "4326";

    @Option(name = "-charset1", usage = "输入数据1字符集")
    private String charset1 = "UTF-8";   // UTF-8, GBK

    @Option(name = "-serialField1", usage = "输入数据1顺序自增字段")
    private String serialField1 = "";

    @Option(name = "-input2", usage = "输入数据2路径", required = true)
    private String input2;

    @Option(name = "-layers2", usage = "输入数据2图层名称")
    private String layers2 = "";
    /**
     * The below is only for specific inputs, not always takes effect.
     */
    @Option(name = "-hasHeader2", usage = "输入数据2是否含有标题行")
    private String hasHeader2 = "true";   // true, false

    @Option(name = "-isWkt2", usage = "输入数据2几何列是否是WKT")
    private String isWkt2 = "true";   // true, false

    @Option(name = "-geometryFields2", usage = "输入数据2几何列")
    private String geometryFields2 = "wkt";   // separate by ","

    @Option(name = "-geometryType2", usage = "输入数据2几何类型")
    private String geometryType2 = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator2", usage = "输入数据2分隔符")
    private String separator2 = "\t";

    @Option(name = "-crs2", usage = "输入数据2地理参考")
    private String crs2 = "4326";

    @Option(name = "-charset2", usage = "输入数据2字符集")
    private String charset2 = "UTF-8";   // UTF-8, GBK

    @Option(name = "-serialField2", usage = "输入数据2顺序自增字段")
    private String serialField2 = "";

    public static MergeArgs initArgs(String[] args) {
        MergeArgs mArgs = new MergeArgs();
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
