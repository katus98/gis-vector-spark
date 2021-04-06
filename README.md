# gis-vector-spark
A framework for basic parallel model of geographic vector data based on Apache Spark.

## Basic GIS model

### Geo Processing Tools

- [x] Buffer (v1.0)
- [x] Clip (v1.0)
- [x] Convex Hull (v1.0)
- [x] Dissolve (v1.0)
- [x] Erase (v1.0)
- [ ] Enlarge
- [x] Intersection (v1.0)
- [x] Symmetrical Difference (v1.0)
- [x] Union (v1.0)

### Geometry Tools

- [ ] Centroids
- [ ] Check Validity
- [ ] Collect
- [ ] Delaunay Triangulation
- [ ] Densify
- [ ] Extract Vertices
- [ ] Geometry Calculator
- [ ] Lines To Polygons
- [x] Multi Part To Single Parts (Multi To Single) (v1.0)
- [ ] Polygons To Lines
- [ ] Simplify
- [ ] Voronoi Polygons

### Attribute Tools

- [x] Join (v1.0)
- [x] Selector
  - [x] Numerical Selector (v1.0)
  - [x] Text Selector (v1.0)
- [x] Statistics (v1.0)
- [ ] Unique Values

### Research Tools

- [ ] Create Grids
- [ ] Random Points
  - [ ] Random Points In Extent
  - [ ] Random Points Inside Polygons
- [x] Random Selection (v1.0)
- [ ] Regular Points

### Analysis Tools

- [ ] Distance Matrix
- [ ] Feature Spatial Count
- [ ] Line Intersections
- [ ] Mean Coordinates
- [ ] Sum Line Lengths

### Data Management Tools

- [x] Merge (v1.0)
- [x] Project (v1.1)
- [x] Spatial Join (v1.0)
- [ ] Split

## GIS Vector Data IO

### Data Input

- [x] ESRI Shape File (v1.0)
- [x] File / Personal Geo Database (v1.2, Need GDAL)
- [x] Relational Database
  - [x] MySQL Database (v1.1)
  - [x] PostgreSQL Database (v1.1)
  - [x] PostgreSQL Databases With Citus (v1.2)
- [x] Text File (v1.0)

### Data Output

- [ ] ESRI Shape File
- [ ] Relational Database
  - [ ] MySQL Database
  - [ ] PostgreSQL Database
- [x] Text File (v1.0)

## User Manual

### Example of program arguments

```shell
# Buffer
-output /D:/Data/buffer.csv
-input postgresql:table
-distance 50
-geometryFields WKT
-crsUnit 3857
-serialField _id

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

## Contributors

### Project Manager

+ SUN Katus

### Model Author

+ SUN Katus
+ WANG Mengxiao