package com.katus.model.at.args;

import com.katus.constant.NumberType;
import com.katus.constant.StatisticalMethod;
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
public class StatisticsArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // true, false

    @Option(name = "-input", usage = "输入目标数据路径", required = true)
    private String input;

    @Option(name = "-layers", usage = "输入目标数据图层名称")
    private String layers = "";

    @Option(name = "-categoryFields", usage = "输入目标数据分类字段")
    private String categoryFields = "";   // separate by ",", result will have one line if empty.

    @Option(name = "-summaryFields", usage = "输入目标数据汇总统计字段", required = true)
    private String summaryFields;   // separate by ","
    /**
     * @see NumberType
     */
    @Option(name = "-numberTypes", usage = "汇总数值字段数据类型", required = true)
    private String numberTypes;   // separate by ","
    /**
     * @see StatisticalMethod
     */
    @Option(name = "-statisticalMethods", usage = "汇总统计方法", required = true)
    private String statisticalMethods;   // separate by ","
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

    public static StatisticsArgs initArgs(String[] args) {
        StatisticsArgs mArgs = new StatisticsArgs();
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
