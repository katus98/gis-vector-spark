package com.katus.io.writer;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.util.fs.FsManipulator;
import com.katus.util.fs.FsManipulatorFactory;
import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-13
 */
@Getter
public class LayerTextFileWriter {
    private final String dirURI;
    private final String fileURI;

    public LayerTextFileWriter(String sharedURI, String dirname, String filename) {
        if (!sharedURI.startsWith("file://") && !sharedURI.startsWith("hdfs://")) sharedURI = "file://" + sharedURI;
        this.dirURI = sharedURI + "/" + dirname;
        this.fileURI = sharedURI + "/" + filename;
    }

    public LayerTextFileWriter(String dirURI, String fileURI) {
        if (!dirURI.startsWith("file://") && !dirURI.startsWith("hdfs://")) dirURI = "file://" + dirURI;
        this.dirURI = dirURI;
        if (!fileURI.startsWith("file://") && !fileURI.startsWith("hdfs://")) fileURI = "file://" + fileURI;
        this.fileURI = fileURI;
    }

    public void writeToDir(Layer layer) throws IOException {   // without title line
        layer.mapToPair(pairItem -> new Tuple2<>(pairItem._1(), pairItem._2().toString()))
                .saveAsHadoopFile(dirURI, String.class, String.class, TextFileOutputFormat.class);
        removeVerificationFile(dirURI);
    }

    public void writeToFileByPartCollect(Layer layer) throws IOException {
        writeToFileByPartCollect(layer, false, false, true);
    }

    public void writeToFileByPartCollect(Layer layer, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        FsManipulator fsManipulator = FsManipulatorFactory.create(fileURI);
        if (fsManipulator.exists(fileURI)) {
            fsManipulator.deleteFile(fileURI);
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsManipulator.write(fileURI, false)));
        if (withHeader) {
            String title = getTitleLine(layer.getMetadata().getFieldNames(), withKey, withGeometry);
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
        removeVerificationFile(fileURI);
    }

    @Deprecated
    public void writeToFileByCollect(Layer layer) throws IOException {
        writeToFileByCollect(layer, true, false, true);
    }

    @Deprecated
    public void writeToFileByCollect(Layer layer, Boolean withHeader, Boolean withKey, Boolean withGeometry) throws IOException {
        List<String> allContent = new ArrayList<>();
        if (withHeader) {
            String title = getTitleLine(layer.getMetadata().getFieldNames(), withKey, withGeometry);
            allContent.add(title);
        }
        List<String> content = getOutputContent(layer, withKey, withGeometry).collect();
        allContent.addAll(content);
        FsManipulator fsManipulator = FsManipulatorFactory.create(fileURI);
        if (fsManipulator.exists(fileURI)) {
            fsManipulator.deleteFile(fileURI);
        }
        fsManipulator.writeTextToFile(fileURI, allContent);
        outputMetadata(layer.getMetadata(), withHeader, withKey, withGeometry);
        removeVerificationFile(fileURI);
    }

    private static String getTitleLine(String[] fieldNames, Boolean withKey, Boolean withGeometry) {
        StringBuilder builder = new StringBuilder();
        if (withKey) {
            builder.append("key\t");
        }
        builder.append("fid\t");
        for (String fieldName : fieldNames) {
            builder.append(fieldName).append("\t");
        }
        if (withGeometry) {
            builder.append("wkt");
        } else {
            builder.deleteCharAt(builder.length() - 1);
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
            builder.append(feature.getFid()).append("\t").append(attrStr);
            if (attrStr.isEmpty()) builder.deleteCharAt(builder.length() - 1);
            if (withGeometry) {
                builder.append("\t").append(feature.getGeometry().toText());
            }
            return builder.toString();
        });
    }

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
        builder.append("Path: ").append(fileURI).append("\n");
        builder.append("Title: ").append(withHeader).append("\n");
        if (withKey) {
            builder.append("The first column is the RDD key. The field column is start on the 2nd column.\n");
        }
        if (withGeometry) {
            builder.append("The last column is the WKT of the geometry.\n");
        }
        builder.append(metadata.toString());
        List<String> content = new ArrayList<>(Collections.singletonList(builder.toString()));
        FsManipulator fsManipulator = FsManipulatorFactory.create(fileURI);
        if (fsManipulator.exists(fileURI + ".meta")) {
            fsManipulator.deleteFile(fileURI + ".meta");
        }
        fsManipulator.writeTextToFile(fileURI + ".meta", content);
    }
}
