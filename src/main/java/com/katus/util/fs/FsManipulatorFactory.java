package com.katus.util.fs;

import org.apache.hadoop.conf.Configuration;

/**
 * 文件系统操作工厂类
 * @author Sun Katus
 * @version 1.0, 2020-09-29
 */
public final class FsManipulatorFactory {
    public static FsManipulator create(String uri) {
        String title = uri.substring(0, uri.indexOf(":"));
        FsManipulator fsManipulator;
        switch (title) {
            case "file":
                fsManipulator = LocalFsManipulator.getInstance();
                break;
            case "hdfs":
                Configuration conf = new Configuration();
                conf.set("dfs.support.append", "true");
                conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
                conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
                fsManipulator = HdfsManipulator.getInstance(uri, conf);
                break;
            default:
                fsManipulator = null;
        }
        return fsManipulator;
    }

    public static FsManipulator create() {
        return FsManipulatorFactory.create("file:///");
    }
}
