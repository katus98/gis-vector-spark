package com.katus.util.fs;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 本地文件系统操作类 (兼容Windows系统和Linux系统)
 * @author Sun Katus
 * @version 1.0, 2020-09-29
 */
public class LocalFsManipulator extends FsManipulator {
    // 静态单例对象
    private static LocalFsManipulator instance;
    private static Configuration configuration;

    /**
     * 私有构造函数 内部类调用 保证单例
     * @param conf 默认 Configuration
     */
    private LocalFsManipulator(Configuration conf) {
        super("file:///", conf);
        DEFAULT_CODEC = new GzipCodec();
        ((GzipCodec) DEFAULT_CODEC).setConf(conf);
    }

    /**
     * 获取 LocalFsManipulator 单例实例 懒汉式单例模式 保证线程安全
     * 如果是Spring工程 请使用Spring注入配置文件中的hdfs uri
     * @return LocalFsManipulator 对象
     */
    protected static LocalFsManipulator getInstance() {
        if (instance == null) {
            synchronized (HdfsManipulator.class) {
                if (instance == null) {
                    configuration = new Configuration();
                    instance = new LocalFsManipulator(configuration);
                }
            }
        }
        return instance;
    }

    /**
     * 将字符串数组转化为File类数组
     * @param paths 字符串路径数组
     * @return File类型数组
     */
    public static File[] stringsToFiles(String[] paths) {
        File[] files = new File[paths.length];
        for (int i = 0; i < paths.length; i++) {
            files[i] = new File(paths[i]);
        }
        return files;
    }

    /**
     * 将多个文件打包为tar文件 本地文件系统特有
     * @param files 文件路径数组
     * @param output tar文件路径
     * @throws IOException io
     */
    public void pack(File[] files, File output) throws IOException {
        FileOutputStream fos = new FileOutputStream(output);
        TarArchiveOutputStream tos = new TarArchiveOutputStream(fos);
        for (File file : files) {
            tos.putArchiveEntry(new TarArchiveEntry(file));
            IOUtils.copy(new FileInputStream(file), tos);
            tos.closeArchiveEntry();
        }
        tos.flush();
        fos.close();
        tos.close();
    }

    public void pack(String[] files, String output) throws IOException {
        this.pack(stringsToFiles(files), new File(output));
    }

    /**
     * 多文件打包压缩zip (仅zip支持多文件打包压缩, 仅限本地文件系统)
     * @param inputs 多文件路径 (禁止包含目录)
     * @param output 压缩文件路径 包含扩展名 (*.zip)
     * @throws IOException io
     */
    public void compress(String[] inputs, String output) throws IOException {
        this.compressToZip(stringsToFiles(inputs), new File(output));
    }

    /**
     * 压缩文件
     * @param input 文件路径
     * @param output 压缩文件路径 包含扩展名 (建议*.zip, 可选*.gz, *.bz2, *.lzo, *.lz4等, 如果扩展名无法识别则使用Gzip算法)
     * @throws IOException io
     */
    @Override
    public void compress(String input, String output) throws IOException {
        if (output.endsWith(".zip")) {
            String[] files = new String[1];
            files[0] = input;
            this.compress(files, output);
            return;
        }
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = factory.getCodec(new Path(output));
        if (codec == null) {
            codec = DEFAULT_CODEC;
        }
        this.compress(new Path(input), new Path(output), codec);
    }

    /**
     * 解压文件
     * @param input 压缩文件 (根据文件扩展名判断文件压缩算法 默认选择Gzip)
     * @param outputDir 输出目录 (实际会在此目录下再次新建目录存储解压的多文件)
     * @return 最终的输出目录
     * @throws IOException io
     */
    @Override
    public String decompress(String input, String outputDir) throws IOException {
        if (input.endsWith(".zip")) {
            return this.decompressFromZip(input, outputDir);
        }
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = factory.getCodec(new Path(input));
        if (codec == null) {
            codec = DEFAULT_CODEC;
        }
        return this.decompress(new Path(input), new Path(outputDir), codec);
    }

    /**
     * 多文件压缩为zip文件
     */
    private void compressToZip(File[] inputFiles, File zipFile) throws IOException {
        if (inputFiles.length <= 0)
            return;
        InputStream is = null;
        ZipArchiveOutputStream zos = new ZipArchiveOutputStream(zipFile);
        zos.setUseZip64(Zip64Mode.AsNeeded);
        for (File file : inputFiles) {
            ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(file, file.getName());
            zos.putArchiveEntry(zipArchiveEntry);
            is = new FileInputStream(file);
            IOUtils.copy(is, zos);
        }
        zos.closeArchiveEntry();
        zos.finish();
        is.close();
        zos.close();
    }

    /**
     * zip文件解压缩
     */
    private String decompressFromZip(String zipFile, String outputDir) throws IOException {
        InputStream is = new FileInputStream(zipFile);
        OutputStream os;
        ZipArchiveInputStream zis = new ZipArchiveInputStream(is);
        ArchiveEntry archiveEntry;
        outputDir = new File(outputDir, new File(zipFile).getName().replace(".zip", "")).getAbsolutePath();
        while (null != (archiveEntry = zis.getNextEntry())) {
            File entryFile = new File(outputDir, archiveEntry.getName());
            if (!entryFile.exists()) {
                boolean mkdirs = entryFile.getParentFile().mkdirs();
            }
            os = new FileOutputStream(entryFile);
            IOUtils.copy(zis, os);
            os.flush();
            os.close();
        }
        zis.close();
        return outputDir;
    }

    /**
     * 字符串序列写出到文件
     * @param file 文件路径
     * @param lines 字符串序列
     * @throws IOException io
     */
    public void writeLines(String file, Iterable<String> lines) throws IOException {
        writeLines(Paths.get(file), lines);
    }

    public void writeLines(java.nio.file.Path path, Iterable<String> lines) throws IOException {
        this.mkdirs(path.getParent().toString());
        Files.write(path, lines);
    }
}
