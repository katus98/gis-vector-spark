package com.katus.entity.data;

import com.katus.constant.GeometryType;
import com.katus.entity.LayerMetadata;
import com.katus.entity.Pyramid;
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
 * @version 1.1, 2020-12-11
 */
@Setter
@Getter
public class Layer extends JavaPairRDD<String, Feature> implements Serializable {
    private static final int DEFAULT_ZOOM = 8;
    private LayerMetadata metadata;
    private boolean isIndexed;

    public Layer(JavaPairRDD<String, Feature> javaPairRDD, LayerMetadata metadata) {
        this(javaPairRDD, metadata, false);
    }

    public Layer(JavaPairRDD<String, Feature> javaPairRDD, LayerMetadata metadata, boolean isIndexed) {
        super(javaPairRDD.rdd(), ClassTag$.MODULE$.apply(String.class), ClassTag$.MODULE$.apply(Feature.class));
        this.metadata = metadata;
        this.isIndexed = isIndexed;
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
                }), this.metadata.copy()
        );
        layer.getMetadata().setCrs(tarCrs);
        return layer;
    }

    /**
     * Create index on layer with grid index
     * @param z zoom level
     * @return new indexed layer
     */
    public Layer index(int z, boolean clip) {
        Pyramid pyramid = new Pyramid(this.metadata.getCrs(), z);
        return new Layer(
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
                                Feature feature;
                                if (clip) {
                                    Geometry interGeometry = polygon.intersection(oriGeom);
                                    feature = new Feature(UUID.randomUUID().toString(), oriFeature.getAttributes(), interGeometry);
                                } else {
                                    feature = new Feature(oriFeature);
                                }
                                result.add(new Tuple2<>(pyramid.getZ() + "-" + x + "-" + y, feature));
                            }
                        }
                    }
                    return result.iterator();
                }), this.getMetadata().copy(), true
        );
    }

    public Layer index() {
        return index(DEFAULT_ZOOM, false);
    }

    /**
     * create a layer
     * @param javaPairRDD layer content
     * @param fields fields of layer
     * @param crs layer's Coordinate Reference System
     * @param geometryType the geometry type of layer (ensure one dimension is ok)
     * @param featureCount the total count of the feature
     * @return new layer
     */
    public static Layer create(JavaPairRDD<String, Feature> javaPairRDD, Field[] fields,
                               CoordinateReferenceSystem crs, GeometryType geometryType, Long featureCount) {
        return new Layer(javaPairRDD, new LayerMetadata("", fields, crs, geometryType, featureCount));
    }
}
