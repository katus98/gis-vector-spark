package com.katus.entity;

import com.katus.constant.CrsExtent;
import lombok.Getter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * @author Sun Katus
 * @version 1.0, 2020-11-13
 */
@Getter
public class Pyramid implements Serializable {
    private CoordinateReferenceSystem crs;
    private Integer z;
    private Double gridSize[];
    private Envelope extent;

    public Pyramid(CoordinateReferenceSystem crs, int z) {
        this.crs = crs;
        this.z = z > 0 && z <= 20 ? z : 10;
        int n = 1 << this.z;
        this.gridSize = new Double[2];
        switch (crs.getName().toString()) {
            case "EPSG:WGS 84 / Pseudo-Mercator":
                this.extent = new Envelope(CrsExtent.EXTENT_3857[0], CrsExtent.EXTENT_3857[1], CrsExtent.EXTENT_3857[2], CrsExtent.EXTENT_3857[3]);
                break;
            case "EPSG:China Geodetic Coordinate System 2000":
                this.extent = new Envelope(CrsExtent.EXTENT_4490[0], CrsExtent.EXTENT_4490[1], CrsExtent.EXTENT_4490[2], CrsExtent.EXTENT_4490[3]);
                break;
            case "EPSG:CGCS2000 / 3-degree Gauss-Kruger zone 40":
                this.extent = new Envelope(CrsExtent.EXTENT_4528[0], CrsExtent.EXTENT_4528[1], CrsExtent.EXTENT_4528[2], CrsExtent.EXTENT_4528[3]);
                break;
            default:
                this.extent = new Envelope(CrsExtent.EXTENT_4326[0], CrsExtent.EXTENT_4326[1], CrsExtent.EXTENT_4326[2], CrsExtent.EXTENT_4326[3]);
                break;
        }
        gridSize[0] = Math.ceil((extent.getMaxX() - extent.getMinX()) / n * 1000) / 1000;
        gridSize[1] = Math.ceil((extent.getMaxY() - extent.getMinY()) / n * 1000) / 1000;
    }

    public Envelope getTile(int numX, int numY) {
        double xMin = extent.getMinX() + numX * gridSize[0];
        double xMax = extent.getMinX() + (numX + 1) * gridSize[0];
        double yMin = extent.getMinY() + numY * gridSize[1];
        double yMax = extent.getMinY() + (numY + 1) * gridSize[1];
        return new Envelope(xMin, xMax, yMin, yMax);
    }

    public Integer[] getTileRange(ReferencedEnvelope envelope) {
        Integer[] range = new Integer[4];
        range[0] = (int) ((envelope.getMinX() - extent.getMinX()) / gridSize[0]);
        range[1] = (int) ((envelope.getMaxX() - extent.getMinX()) / gridSize[0]);
        range[2] = (int) ((envelope.getMinY() - extent.getMinY()) / gridSize[1]);
        range[3] = (int) ((envelope.getMaxY() - extent.getMinY()) / gridSize[1]);
        return range;
    }
}
