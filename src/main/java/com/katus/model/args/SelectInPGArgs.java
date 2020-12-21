package com.katus.model.args;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author Sun Katus
 * @version 1.0, 2020-12-21
 * @since 1.2
 */
@Getter
@Setter
@Slf4j
public class SelectInPGArgs {
    @Option(name = "-output", usage = "输出文件路径", required = true)
    private String output;

    @Option(name = "-needHeader", usage = "输出文件是否含有标题行")
    private String needHeader = "true";   // true, false

    @Option(name = "-input", usage = "输入目标数据路径", required = true)
    private String input;

    @Option(name = "-expression", usage = "输入目标数据筛选表达式", required = true)
    private String expression;

    @Option(name = "-serialField", usage = "输入目标数据顺序自增字段")
    private String serialField = "_id";

    public static SelectInPGArgs initArgs(String[] args) {
        SelectInPGArgs mArgs = new SelectInPGArgs();
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
