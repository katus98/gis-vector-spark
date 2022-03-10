package com.katus.io.writer;

import com.katus.entity.data.Feature;
import com.katus.entity.data.Field;
import com.katus.entity.data.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Sun Katus
 * @version 1.1, 2020-12-21
 */
@Getter
@Slf4j
public class LayerTextFileWriter implements Serializable {
    private final String pathURI;

    public LayerTextFileWriter(String path) {
        if (!path.startsWith("file://") && !path.startsWith("hdfs://")) path = "file://" + path;
        this.pathURI = path;
    }

    private String initDir() throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create(pathURI);
        if (fsManipulator.exists(pathURI)) fsManipulator.deleteDir(pathURI);
        fsManipulator.mkdirs(pathURI);
        return pathURI.substring(pathURI.lastIndexOf("/") + 1);
    }

    public void writeToDir(Layer layer) throws IOException {   // without title line
        layer.mapToPair(pairItem -> new Tuple2<>(pairItem._1(), pairItem._2().toString()))
                .saveAsHadoopFile(pathURI, String.class, String.class, TextFileOutputFormat.class);
        outputMetadata(layer.getMetadata(), false, false, true);
    }

    public void writeToDirByMap(Layer layer) throws IOException {
        writeToDirByMap(layer, true, false, true);
    }

    public void writeToDirByMap(Layer layer, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        String filename = initDir();
        String[] fieldNames = Arrays.stream(layer.getMetadata().getFields()).map(Field::toString).toArray(String[]::new);
        JavaRDD<String> outputContent = getOutputContent(layer, withKey, withGeometry);
        List<String> outputInfo = outputContent.mapPartitionsWithIndex((index, it) -> {
            List<String> result = new ArrayList<>();
            String parFileURI = pathURI + "/" + filename + "-" + index + ".tsv";
            try {
                FsManipulator fsManipulator = FsManipulatorFactory.create(parFileURI);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsManipulator.write(parFileURI, false)));
                if (withHeader) {
                    String title = getTitleLine(fieldNames, withKey, withGeometry);
                    writer.write(title + "\n");
                }
                while (it.hasNext()) {
                    writer.write(it.next() + "\n");
                }
                writer.flush();
                writer.close();
                result.add(parFileURI + ": Succeeded!");
            } catch (IOException e) {
                result.add(parFileURI + ": Failed!");
            }
            return result.iterator();
        }, false).collect();
        outputInfo.forEach(log::info);
        outputMetadata(layer.getMetadata(), withHeader, withKey, withGeometry);
    }

    public void writeToDirByMap(Dataset<Row> dataset) throws IOException {
        String filename = initDir();
        String[] fieldNames = dataset.columns();
        List<String> outputInfo = dataset.toJavaRDD().map(row -> {
            StringBuilder builder = new StringBuilder();
            for (String fieldName : fieldNames) {
                builder.append(row.get(row.fieldIndex(fieldName))).append("\t");
            }
            builder.deleteCharAt(builder.length() - 1);
            return builder.toString();
        }).mapPartitionsWithIndex((index, it) -> {
            List<String> result = new ArrayList<>();
            String parFileURI = pathURI + "/" + filename + "-" + index + ".tsv";
            try {
                FsManipulator fsManipulator = FsManipulatorFactory.create(parFileURI);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsManipulator.write(parFileURI, false)));
                String title = getTitleLine(fieldNames, false, false);
                writer.write(title + "\n");
                while (it.hasNext()) {
                    writer.write(it.next() + "\n");
                }
                writer.flush();
                writer.close();
                result.add(parFileURI + ": Succeeded!");
            } catch (IOException e) {
                result.add(parFileURI + ": Failed!");
            }
            return result.iterator();
        }, false).collect();
        outputInfo.forEach(log::info);
    }

    public void writeToFileByPartCollect(Layer layer) throws IOException {
        writeToFileByPartCollect(layer, true, false, true);
    }

    public void writeToFileByPartCollect(Layer layer, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create(pathURI);
        if (fsManipulator.exists(pathURI)) {
            fsManipulator.deleteFile(pathURI);
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsManipulator.write(pathURI, false)));
        if (withHeader) {
            String[] fieldNames = Arrays.stream(layer.getMetadata().getFields()).map(Field::toString).toArray(String[]::new);
            String title = getTitleLine(fieldNames, withKey, withGeometry);
            writer.write(title + "\n");
        }
        JavaRDD<String> outputContent = getOutputContent(layer, withKey, withGeometry);
        for (int i = 0; i < outputContent.getNumPartitions(); i++) {
            List<String>[] partContent = outputContent.collectPartitions(new int[]{i});
            for (String line : partContent[0]) {
                writer.write(line + "\n");
            }
        }
        writer.flush();
        writer.close();
        outputMetadata(layer.getMetadata(), withHeader, withKey, withGeometry);
    }

    @Deprecated
    public void writeToFileByCollect(Layer layer) throws IOException {
        writeToFileByCollect(layer, true, false, true);
    }

    @Deprecated
    public void writeToFileByCollect(Layer layer, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        List<String> allContent = new ArrayList<>();
        if (withHeader) {
            String[] fieldNames = Arrays.stream(layer.getMetadata().getFields()).map(Field::toString).toArray(String[]::new);
            String title = getTitleLine(fieldNames, withKey, withGeometry);
            allContent.add(title);
        }
        List<String> content = getOutputContent(layer, withKey, withGeometry).collect();
        allContent.addAll(content);
        FsManipulator fsManipulator = FsManipulatorFactory.create(pathURI);
        if (fsManipulator.exists(pathURI)) {
            fsManipulator.deleteFile(pathURI);
        }
        fsManipulator.writeTextToFile(pathURI, allContent);
        outputMetadata(layer.getMetadata(), withHeader, withKey, withGeometry);
    }

    private static String getTitleLine(String[] fieldNames, Boolean withKey, Boolean withGeometry) {
        StringBuilder builder = new StringBuilder();
        if (withKey) {
            builder.append("key\t");
        }
        for (String fieldName : fieldNames) {
            builder.append(fieldName).append("\t");
        }
        if (withGeometry) {
            builder.append("wkt");
        } else {
            if (builder.length() > 0) builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private static JavaRDD<String> getOutputContent(Layer layer, Boolean withKey, Boolean withGeometry) {
        return layer.map(pairItem -> {
            Feature feature = pairItem._2();
            StringBuilder builder = new StringBuilder();
            if (withKey) {
                builder.append(pairItem._1()).append("\t");
            }
            String attrStr = feature.showAttributes();
            builder.append(attrStr);
            if (withKey && attrStr.isEmpty()) builder.deleteCharAt(builder.length() - 1);
            if (withGeometry) {
                builder.append("\t").append(feature.getGeometry().toText());
            }
            return builder.toString();
        });
    }

    @Deprecated
    private static void removeVerificationFile(String uri) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create(uri);
        String crcURI;
        if (fsManipulator.isFile(uri)) {
            crcURI = getVerificationFileURI(uri);
            if (fsManipulator.exists(crcURI)) {
                fsManipulator.deleteFile(crcURI);
            }
            crcURI = getVerificationFileURI(uri + ".meta");
            if (fsManipulator.exists(crcURI)) {
                fsManipulator.deleteFile(crcURI);
            }
        } else {
            Path[] paths = fsManipulator.listFiles(uri);
            for (Path path : paths) {
                crcURI = getVerificationFileURI(path.toString());
                if (fsManipulator.exists(crcURI)) {
                    fsManipulator.deleteFile(crcURI);
                }
            }
        }
    }

    private static String getVerificationFileURI(String uri) {
        return uri.substring(0, uri.lastIndexOf("/") + 1) + "." + uri.substring(uri.lastIndexOf("/") + 1) + ".crc";
    }

    private void outputMetadata(LayerMetadata metadata, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("******Output Info******\n");
        builder.append("Path: ").append(pathURI).append("\n");
        builder.append("Title: ").append(withHeader).append("\n");
        if (withKey) {
            builder.append("The first column is the RDD key. The field column is start on the 2nd column.\n");
        }
        if (withGeometry) {
            builder.append("The last column is the WKT of the geometry.\n");
        }
        builder.append(metadata.toString());
        List<String> content = new ArrayList<>(Collections.singletonList(builder.toString()));
        FsManipulator fsManipulator = FsManipulatorFactory.create(pathURI);
        if (fsManipulator.exists(pathURI + ".meta")) {
            fsManipulator.deleteFile(pathURI + ".meta");
        }
        fsManipulator.writeTextToFile(pathURI + ".meta", content);
    }
}
