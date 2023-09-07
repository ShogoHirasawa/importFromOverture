import duckdb
import geopandas as gpd

# DB の作成
conn = duckdb.connect("../overture.duckdb")
c = conn.cursor()

c.execute(
    """INSTALL spatial;
    INSTALL httpfs;
    LOAD spatial;
    LOAD parquet;
"""
)

# Salinasの建物データだけを抽出し、FlatGeobufフォーマットで出力

c.execute(
    """COPY (
    SELECT
           theme,
           type,
           version,
           updateTime,
           JSON(sources) AS sources,
           JSON(names) AS names,
           height,
           numFloors,
           class,
           ST_GeomFromWkb(geometry) AS geometry
      FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=buildings/type=*/*')
     WHERE ST_Within(ST_GeomFromWkb(geometry), ST_Envelope(ST_GeomFromText('POLYGON((-81.0575 -2.3667,-81.0555 -2.1611,-80.7804 -2.1437,-80.7785 -2.3854,-81.0575 -2.3667))')))
) TO '../buildings.fgb'
WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');
"""
)

## GeoDataFrameに変換して最初の5行だけ表示

gdf = gpd.read_parquet("../buildings.fgb")
gdf.head()

