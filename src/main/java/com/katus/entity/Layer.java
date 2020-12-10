package com.katus.entity;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-05
 */
@Setter
@Getter
public class Layer extends JavaPairRDD<String, Feature> implements Serializable {
    private LayerMetadata metadata;
    private Boolean isIndexed = false;

    private Layer(JavaPairRDD<String, Feature> javaPairRDD) {
        super(javaPairRDD.rdd(), ClassTag$.MODULE$.apply(String.class), ClassTag$.MODULE$.apply(Feature.class));
    }

    /**
     * Transform layer's Coordinate Reference System
     * @param tarCrs target crs
     * @return new projected layer
     */
    public Layer project(CoordinateReferenceSystem tarCrs) {
        CoordinateReferenceSystem oriCrs = metadata.getCrs();
        Layer layer = new Layer(
                this.mapToPair(pairItem -> {
                    pairItem._2().transform(oriCrs, tarCrs);
                    return pairItem;
                })
        );
        layer.setMetadata(this.getMetadata());
        layer.getMetadata().setCrs(tarCrs);
        return layer;
    }

    /**
     * Create index on layer with grid index
     * @param z zoom level
     * @return new indexed layer
     */
    public Layer index(int z) {
        Pyramid pyramid = new Pyramid(this.metadata.getCrs(), z);
        Layer layer = new Layer(
                this.flatMapToPair(pairItem -> {
                    List<Tuple2<String, Feature>> result = new ArrayList<>();
                    Feature oriFeature = pairItem._2();
                    Geometry oriGeom = oriFeature.getGeometry();
                    ReferencedEnvelope envelope = JTS.toEnvelope(oriGeom);
                    Integer[] range = pyramid.getTileRange(envelope);
                    for (int x = range[0]; x <= range[1]; x++) {
                        for (int y = range[2]; y <= range[3]; y++) {
                            Polygon polygon = JTS.toGeometry(pyramid.getTile(x, y));
                            if (polygon.intersects(oriGeom)) {
                                Geometry interGeometry = polygon.intersection(oriGeom);
                                Feature feature = new Feature(UUID.randomUUID().toString(), oriFeature.getAttributes(), interGeometry);
                                result.add(new Tuple2<>(pyramid.getZ() + "-" + x + "-" + y, feature));
                            }
                        }
                    }
                    return result.iterator();
                })
        );
        layer.setMetadata(this.getMetadata());
        layer.setIsIndexed(true);
        return layer;
    }

    /**
     * create a layer
     * @param javaPairRDD layer content
     * @param fieldNames fields of layer
     * @param crs layer's Coordinate Reference System
     * @param geometryType the geometry type of layer (ensure one dimension is ok)
     * @param featureCount the total count of the feature
     * @return new layer
     */
    public static Layer create(JavaPairRDD<String, Feature> javaPairRDD,
                                                   String[] fieldNames, CoordinateReferenceSystem crs, String geometryType,
                                                   Long featureCount) {
        Layer layer = new Layer(javaPairRDD);
        layer.setMetadata(new LayerMetadata(fieldNames, crs, geometryType, featureCount));
        return layer;
    }

    public static Layer create(JavaPairRDD<String, Feature> javaPairRDD,
                                            String[] fieldNames, CoordinateReferenceSystem crs, String geometryType) {
        return create(javaPairRDD, fieldNames, crs, geometryType, -1L);
    }

    /**
     * create a layer
     * @param javaPairRDD layer content
     * @param metadata the metadata of the layer
     * @param isIndexed true if the layer is indexed
     * @return new layer
     */
    public static Layer create(JavaPairRDD<String, Feature> javaPairRDD, LayerMetadata metadata, Boolean isIndexed) {
        Layer layer = new Layer(javaPairRDD);
        layer.setMetadata(metadata);
        layer.setIsIndexed(isIndexed);
        return layer;
    }

    public static Layer create(JavaPairRDD<String, Feature> javaPairRDD, LayerMetadata metadata) {
        return create(javaPairRDD, metadata, false);
    }
}
