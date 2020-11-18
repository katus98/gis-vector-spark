package com.katus.model.args;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
@Getter
@Setter
@Slf4j
public class MultiToSingleArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-input", usage = "输入目标数据路径", required = true)
    private String input;
    /**
     * The below is only for text file
     */
    @Option(name = "-hasHeader", usage = "输入目标数据是否含有标题行")
    private String hasHeader = "false";   // false, true

    @Option(name = "-isWkt", usage = "输入目标数据几何列是否是WKT")
    private String isWkt = "true";   // true, false

    @Option(name = "-geometryFields", usage = "输入目标数据几何列")
    private String geometryFields = "-1";   // separate by ","

    @Option(name = "-geometryType", usage = "输入目标数据几何列")
    private String geometryType = "LineString";   // Polygon, LineString, Point

    @Option(name = "-separator", usage = "输入目标数据分隔符")
    private String separator = "\t";

    @Option(name = "-crs", usage = "输入目标数据地理参考")
    private String crs = "4326";   // 4326, 3857

    @Option(name = "-charset", usage = "输入目标数据字符集")
    private String charset = "UTF-8";   // UTF-8, GBK

    public static MultiToSingleArgs initArgs(String[] args) {
        MultiToSingleArgs mArgs = new MultiToSingleArgs();
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
