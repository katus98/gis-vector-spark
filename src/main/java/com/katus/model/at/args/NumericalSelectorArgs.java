package com.katus.model.at.args;

import com.katus.constant.NumberRelationship;
import com.katus.constant.NumberType;
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
public class NumericalSelectorArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // true, false

    @Option(name = "-input", usage = "输入目标数据路径", required = true)
    private String input;

    @Option(name = "-layers", usage = "输入目标数据图层名称")
    private String layers = "";

    @Option(name = "-selectField", usage = "输入目标数据筛选数值字段", required = true)
    private String selectField;
    /**
     * @see NumberRelationship
     */
    @Option(name = "-numberRelationship", usage = "用于筛选的数值关系")
    private String numberRelationship = "==";
    /**
     * @see NumberType
     */
    @Option(name = "-numberType", usage = "数值字段数据类型")
    private String numberType = "Double";

    @Option(name = "-threshold", usage = "筛选阈值", required = true)
    private String threshold;
    /**
     * The below is only for specific inputs, not always takes effect.
     */
    @Option(name = "-hasHeader", usage = "输入目标数据是否含有标题行")
    private String hasHeader = "true";   // true, false

    @Option(name = "-isWkt", usage = "输入目标数据几何列是否是WKT")
    private String isWkt = "true";   // true, false

    @Option(name = "-geometryFields", usage = "输入目标数据几何列")
    private String geometryFields = "wkt";   // separate by ","

    @Option(name = "-geometryType", usage = "输入目标数据几何类型")
    private String geometryType = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator", usage = "输入目标数据分隔符")
    private String separator = "\t";

    @Option(name = "-crs", usage = "输入目标数据地理参考")
    private String crs = "4326";

    @Option(name = "-charset", usage = "输入目标数据字符集")
    private String charset = "UTF-8";   // UTF-8, GBK

    @Option(name = "-serialField", usage = "输入目标数据顺序自增字段")
    private String serialField = "";

    public static NumericalSelectorArgs initArgs(String[] args) {
        NumericalSelectorArgs mArgs = new NumericalSelectorArgs();
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
