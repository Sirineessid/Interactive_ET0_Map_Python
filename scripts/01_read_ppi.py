import os
import psycopg2
import geopandas as gpd
import geohash2

DB_CONFIG = {
    "dbname": "nasa_et0",
    "user": "postgres",
    "password": "Sirine123",
    "host": "localhost",
    "port": 5432
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHP_PATH = os.path.join(BASE_DIR, "data/ppi", "ppi_piait.shp")
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "ppi_points.geojson")

if not os.path.exists(SHP_PATH):
    raise FileNotFoundError(f"{SHP_PATH} not found.")

# 1️⃣ Read shapefile
gdf = gpd.read_file(SHP_PATH)

# 2️⃣ Filter JENDOUBA (uppercase safe)
gdf = gdf[gdf["PPI_GOV"].str.upper() == "JENDOUBA"]

# 3️⃣ Reproject to UTM (meters) for accurate centroid
gdf_proj = gdf.to_crs(epsg=32632)

# 4️⃣ Compute centroid
gdf_proj["geometry"] = gdf_proj.geometry.centroid

# 5️⃣ Convert back to WGS84 (lat/lon)
gdf = gdf_proj.to_crs(epsg=4326)

# 6️⃣ Export corrected GeoJSON (real map coordinates)
gdf.to_file(OUTPUT_PATH, driver="GeoJSON")

# 7️⃣ Insert into PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

for _, row in gdf.iterrows():
    lat = row.geometry.y
    lon = row.geometry.x
    geoh = geohash2.encode(lat, lon, precision=7)

    cur.execute("""
        INSERT INTO ppi_points (ppi_nom, gov_name, lat, lon, geohash)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (geohash) DO NOTHING
    """, (
        row["PPI_NOM"],
        row["PPI_GOV"],
        lat,
        lon,
        geoh
    ))

conn.commit()
conn.close()

print("JENDOUBA PPI exported and inserted successfully.")
