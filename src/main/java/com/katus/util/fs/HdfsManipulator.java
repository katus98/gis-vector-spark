package com.katus.util.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;

/**
 * 分布式文件系统操作类 (HDFS等)
 * @author Sun Katus
 * @version 1.0, 2020-09-28
 */
public class HdfsManipulator extends FsManipulator {
    // 静态单例对象
    private static HdfsManipulator instance;
    private static Configuration configuration;

    /**
     * 私有构造函数 内部类调用 保证单例
     * @param uri Hadoop URI
     * @param conf Hadoop Configuration
     */
    private HdfsManipulator(String uri, Configuration conf) {
        super(uri, conf);
        DEFAULT_CODEC = new BZip2Codec();
        ((BZip2Codec) DEFAULT_CODEC).setConf(conf);
    }

    /**
     * 获取 HdfsManipulator 单例实例 懒汉式单例模式 保证线程安全
     * 如果是Spring工程 请使用Spring注入配置文件中的hdfs uri
     * @return HdfsManipulator 对象
     */
    protected static HdfsManipulator getInstance(String uri, Configuration conf) {
        if (instance == null) {
            synchronized (HdfsManipulator.class) {
                if (instance == null) {
                    configuration = conf;
                    instance = new HdfsManipulator(uri, configuration);
                }
            }
        }
        return instance;
    }

    /**
     * 压缩文件
     * 默认使用BZip2压缩格式
     */
    @Override
    public void compress(String input, String output) throws IOException {
        this.compress(new Path(input), new Path(output), DEFAULT_CODEC);
    }

    /**
     * 解压文件
     * 首先使用扩展名推测压缩类型 如果无法判断默认使用BZip2解压算法
     */
    @Override
    public String decompress(String input, String outputDir) throws IOException {
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = factory.getCodec(new Path(input));
        if (codec == null) {
            codec = DEFAULT_CODEC;
        }
        return this.decompress(new Path(input), new Path(outputDir), codec);
    }

    /**
     * 从本地文件系统向hdfs拷贝文件/目录
     * @param src 本地路径
     * @param tar hdfs路径
     * @return 是否拷贝成功
     */
    public boolean uploadFromLocal(Path src, Path tar) {
        try {
            fs.copyFromLocalFile(src, tar);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean uploadFromLocal(String src, String tar) {
        return this.uploadFromLocal(new Path(src), new Path(tar));
    }

    /**
     * 从hdfs系统向本地文件系统拷贝文件/目录
     * @param src hdfs路径
     * @param tar 本地路径
     * @return 是否拷贝成功
     */
    public boolean downloadToLocal(Path src, Path tar) {
        try {
            fs.copyToLocalFile(src, tar);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean downloadToLocal(String src, String tar) {
        return this.downloadToLocal(new Path(src), new Path(tar));
    }
}
