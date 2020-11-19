package com.katus.model.args;

import com.katus.constant.TextRelationship;
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
public class FieldTextSelectorArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // false, true

    @Option(name = "-input", usage = "输入目标数据路径", required = true)
    private String input;

    @Option(name = "-selectField", usage = "输入目标数据筛选字符串字段", required = true)
    private String selectField;
    /**
     * @see TextRelationship
     */
    @Option(name = "-textRelationship", usage = "用于筛选字符串的关系")
    private String textRelationship = "equal";   // equal, contain, start_with, end_with

    @Option(name = "-keywords", usage = "筛选关键字", required = true)
    private String keywords;   // separate by ","
    /**
     * The below is only for text file
     */
    @Option(name = "-hasHeader", usage = "输入目标数据是否含有标题行")
    private String hasHeader = "false";   // false, true

    @Option(name = "-isWkt", usage = "输入目标数据几何列是否是WKT")
    private String isWkt = "true";   // true, false

    @Option(name = "-geometryFields", usage = "输入目标数据几何列")
    private String geometryFields = "-1";   // separate by ","

    @Option(name = "-geometryType", usage = "输入目标数据几何类型")
    private String geometryType = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator", usage = "输入目标数据分隔符")
    private String separator = "\t";

    @Option(name = "-crs", usage = "输入目标数据地理参考")
    private String crs = "4326";   // 4326, 3857

    @Option(name = "-charset", usage = "输入目标数据字符集")
    private String charset = "UTF-8";   // UTF-8, GBK

    public static FieldTextSelectorArgs initArgs(String[] args) {
        FieldTextSelectorArgs mArgs = new FieldTextSelectorArgs();
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
