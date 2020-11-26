package com.katus.util.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * File System Manipulator
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-29
 */
public abstract class FsManipulator implements Closeable {
    protected final FileSystem fs;
    protected CompressionCodec DEFAULT_CODEC;

    protected FsManipulator(String uri, Configuration conf) {
        try {
            this.fs = FileSystem.get(URI.create(uri), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 判断路径是否存在
     * @param path 路径
     * @return 是否存在
     * @throws IOException io
     */
    public boolean exists(Path path) throws IOException {
        return fs.exists(path);
    }

    public boolean exists(String path) throws IOException {
        return this.exists(new Path(path));
    }

    /**
     * 判断路径是否为文件
     * @param path 路径
     * @return 是否为文件
     * @throws IOException io
     */
    public boolean isFile(Path path) throws IOException {
        return fs.isFile(path);
    }

    public boolean isFile(String path) throws IOException {
        return this.isFile(new Path(path));
    }

    /**
     * 列出路径下所有子路径 (可能无法获取隐藏文件)
     * @param path 路径
     * @param filter 路径过滤器 返回满足条件的路径
     * @return 成员类型为 Path 的数组
     * @throws IOException io
     */
    public Path[] listFiles(Path path, PathFilter filter) throws IOException {
        FileStatus[] fileStatuses = filter == null ? fs.listStatus(path) : fs.listStatus(path, filter);
        Path[] paths = new Path[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath();
        }
        return paths;
    }

    public Path[] listFiles(Path path) throws IOException {
        return listFiles(path, null);
    }

    public Path[] listFiles(String path) throws IOException {
        return listFiles(new Path(path));
    }

    /**
     * 创建多级目录
     * @param dir 创建的目录
     * @return 是否创建成功
     * @throws IOException io
     */
    public boolean mkdirs(Path dir) throws IOException {
        return fs.mkdirs(dir);
    }

    public boolean mkdirs(String dir) throws IOException {
        return this.mkdirs(new Path(dir));
    }

    /**
     * 创建空文件
     * @param file 文件路径
     * @return 是否创建成功
     * @throws IOException io
     */
    public boolean createFile(Path file) throws IOException {
        return fs.createNewFile(file);
    }

    public boolean createFile(String file) throws IOException {
        return this.createFile(new Path(file));
    }

    /**
     * 删除目录 (目录均使用递归删除目录及其下全部文件)
     * @param dir 目录路径
     * @throws IOException io
     */
    public void deleteDir(Path dir) throws IOException {
        fs.delete(dir, true);
    }

    public void deleteDir(String dir) throws IOException {
        this.deleteDir(new Path(dir));
    }

    public void deleteDirs(String...dirs) throws IOException {
        for (String dir : dirs) {
            this.deleteDir(dir);
        }
    }

    /**
     * 删除文件 (如果实际为目录且其下有文件则删除失败)
     * @param file 文件路径
     * @throws IOException io
     */
    public void deleteFile(Path file) throws IOException {
        fs.delete(file, false);
    }

    public void deleteFile(String file) throws IOException {
        this.deleteFile(new Path(file));
    }

    public void deleteFiles(String...files) throws IOException {
        for (String file : files) {
            this.deleteFile(file);
        }
    }

    /**
     * 读取文件 获取文件输入流
     * @param path 文件路径
     * @return 输入流
     * @throws IOException io
     */
    public FSDataInputStream read(Path path) throws IOException {
        return fs.open(path);
    }

    public FSDataInputStream read(String path) throws IOException {
        return this.read(new Path(path));
    }

    /**
     * 写出到文件 获取文件写出输出流
     * @param path 文件路径
     * @param overwrite 是否覆写
     * @return 写出输出流
     * @throws IOException io
     */
    public FSDataOutputStream write(Path path, boolean overwrite) throws IOException {
        return fs.create(path, overwrite);
    }

    public FSDataOutputStream write(String path, boolean overwrite) throws IOException {
        return this.write(new Path(path), overwrite);
    }

    /**
     * 追加到文件 获取文件追加输出流
     * @param path 文件路径
     * @return 追加输出流
     * @throws IOException io
     */
    public FSDataOutputStream append(Path path) throws IOException {
        return fs.append(path);
    }

    public FSDataOutputStream append(String path) throws IOException {
        return this.append(new Path(path));
    }

    /**
     * 复制文件
     * @param src 原始路径
     * @param dest 新路径
     * @param overwrite 是否覆写
     * @throws IOException io
     */
    public void copy(Path src, Path dest, boolean overwrite) throws IOException {
        FSDataInputStream is = this.read(src);
        FSDataOutputStream os = this.write(dest, overwrite);
        this.copyBytes(is, os);
    }

    public void copy(String src, String dest, boolean overwrite) throws IOException {
        this.copy(new Path(src), new Path(dest), overwrite);
    }

    /**
     * 执行 IO (从输入流向输出流复制字节)
     * @param is 输入流
     * @param os 输出流
     * @param bufferSize 缓冲大小(单位为字节)
     * @throws IOException io
     */
    public void copyBytes(InputStream is, OutputStream os, int bufferSize) throws IOException {
        IOUtils.copyBytes(is, os, bufferSize, true);
    }

    public void copyBytes(InputStream is, OutputStream os) throws IOException {
        this.copyBytes(is, os, 4096);
    }

    /**
     * 文件重命名
     * @param src 原路径
     * @param dest 新路径
     * @return 是否成功
     * @throws IOException io
     */
    public boolean rename(Path src, Path dest) throws IOException {
        return fs.rename(src, dest);
    }

    public boolean rename(String src, String dest) throws IOException {
        return this.rename(new Path(src), new Path(dest));
    }

    /**
     * 压缩文件 (仅限文件)
     * @param input 文件路径
     * @param output 文件输出路径
     * @param codec 压缩算法接口实现类
     * @throws IOException io
     */
    public void compress(Path input, Path output, CompressionCodec codec) throws IOException {
        InputStream is = this.read(input);
        if (!output.toString().endsWith(codec.getDefaultExtension())) {
            output = new Path(output.toString() + codec.getDefaultExtension());
        }
        CompressionOutputStream cos = codec.createOutputStream(this.write(output, true));
        this.copyBytes(is, cos);
    }

    public abstract void compress(String input, String output) throws IOException;

    /**
     * 解压文件
     * @param input 压缩文件路径
     * @param outputDir 解压文件输出路径
     * @param codec 压缩算法接口实现类
     * @throws IOException io
     */
    public String decompress(Path input, Path outputDir, CompressionCodec codec) throws IOException {
        InputStream is = codec.createInputStream(this.read(input));
        String dirName = CompressionCodecFactory.removeSuffix(input.getName(), codec.getDefaultExtension());
        outputDir = new Path(outputDir.toString(), dirName);
        if (this.exists(outputDir)) {
            this.mkdirs(outputDir);
        }
        OutputStream os = this.write(outputDir, true);
        this.copyBytes(is, os);
        return outputDir.toString();
    }

    public abstract String decompress(String input, String outputDir) throws IOException;

    /**
     * 获取 Home 目录
     * @return Home 目录
     */
    public String getHomeDirectory() {
        return fs.getHomeDirectory().toString();
    }

    /**
     * 获取文件/目录的修改时间字符串
     * @param path 路径
     * @return 时间字符串
     * @throws IOException io
     */
    public String modificationTime(Path path) throws IOException {
        return new Date(fs.getFileStatus(path).getModificationTime()).toString();
    }

    public String modificationTime(String path) throws IOException {
        return this.modificationTime(new Path(path));
    }

    /**
     * 获取文件大小字符串 (单位为字节)
     * @param file 文件路径
     * @return 如果是目录为 None
     * @throws IOException io
     */
    public String size(Path file) throws IOException {
        return this.isFile(file) ? String.valueOf(fs.getFileStatus(file).getLen()) : "None";
    }

    public String size(String file) throws IOException {
        return this.size(new Path(file));
    }

    /**
     * 从文件中按行读取文本
     * @param filename 文件路径
     * @param size 读取行数
     * @param charset 解码字符集
     * @return 文本列表
     * @throws IOException io
     */
    public List<String> readToText(String filename, int size, Charset charset) throws IOException {
        List<String> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.read(filename), charset));
        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
            if (lines.size() >= size) break;
        }
        reader.close();
        return lines;
    }

    public List<String> readToText(String filename, int size) throws IOException {
        return this.readToText(filename, size, StandardCharsets.UTF_8);
    }

    public List<String> readToText(String filename) throws IOException {
        return this.readToText(filename, 10);
    }

    /**
     * 将文本写入文件
     * @param filename 文件路径
     * @param content 内容
     * @param charset 编码字符集
     * @throws IOException io
     */
    public void writeTextToFile(String filename, List<String> content, Charset charset) throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.write(filename, false), charset));
        for (String line : content) {
            writer.write(line + "\n");
        }
        writer.close();
    }

    public void writeTextToFile(String filename, List<String> content) throws IOException {
        this.writeTextToFile(filename, content, StandardCharsets.UTF_8);
    }

    /**
     * 关闭fs 工程中请务必不要调用本方法 否则单例对象中的fs将被关闭 本工具类将失效
     * 本方法本质上是对工具类的一个完善 如果非单例对象 则需要在每次使用结束后调用 保证内存的有效回收
     * @throws IOException io
     */
    @Override
    public void close() throws IOException {
        // 通过反射机制判断本类是否为单例对象
        boolean isSingleton = false;
        Field[] fields = FsManipulator.class.getDeclaredFields();
        for (Field field : fields) {
            // 根据本类中是否含有本类的静态对象来判断是否为单例模式
            if (FsManipulator.class.equals(field.getType()) && Modifier.isStatic(field.getModifiers())) {
                isSingleton = true;
                break;
            }
        }
        if (!isSingleton) {   // 只有不是单例对象才能关闭hdfs
            fs.close();
        }
    }
}
