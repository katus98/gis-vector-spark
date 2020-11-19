# gis-vector-spark
A framework for basic parallel model of geographic vector data based on Apache Spark.

## Basic GIS model

### Geo Processing

- [ ] Buffer
- [x] Clip
- [x] Convex Hull
- [x] Dissolve
- [x] Erase
- [x] Intersection
- [x] Symmetrical Difference
- [ ] Union

### Geometry Tools

- [x] Multi Part To Single Parts (Multi To Single)
- [ ] Polygons To Lines
- [ ] Lines To Polygons

### Field Tools

- [x] Field Selector
  - [x] Field Text Selector
  - [x] Field Numerical Selector
- [x] Field Statistics
- [x] Field Join

### Research Tools

- [x] Random Selection

### Analysis Tools

- [ ] Feature Spatial Count

### Data Management Tools

- [x] Merge
- [x] Spatial Join

## User Manual

### Example of program arguments

```shell
# Clip
-output /D:/Data/clip.csv
-needHeader true
-crs 4326
-input1 /D:/Data/target.shp
-hasHeader1 false
-isWkt1 true
-geometryFields1 -1
-geometryType1 LineString
-separator1 \t
-crs1 4326
-charset1 UTF-8
-input2 /D:/Data/extent.shp
-hasHeader2 false
-isWkt2 true
-geometryFields2 -1
-geometryType2 LineString
-separator2 \t
-crs2 4326
-charset2 UTF-8
# MultiToSingle
-output /D:/Data/mts.csv
-needHeader true
-crs 4326
-input /D:/Data/target.shp
-ratio 0.5
-hasHeader false
-isWkt true
-geometryFields -1
-geometryType LineString
-separator \t
-crs 4326
-charset UTF-8
```

