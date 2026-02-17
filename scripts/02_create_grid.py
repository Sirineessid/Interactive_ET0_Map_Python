import psycopg2
from shapely.geometry import box, mapping
import geohash2
import json

# -----------------------------
# Database configuration
# -----------------------------
DB_CONFIG = {
    "dbname": "nasa_et0",
    "user": "postgres",
    "password": "Sirine123",
    "host": "127.0.0.1",
    "port": "5432"
}

# -----------------------------
# Grid settings
# -----------------------------
GRID_SIZE = 0.001  # ~100m in degrees
GEOJSON_FILE = "output/grid_100m.geojson"

# -----------------------------
# PPI points for Jendouba
# -----------------------------
ppi_points = [
    {"name": "Bouhertma 3", "coordinates": [36.572811, 8.996081]},  # lat, lon
    {"name": "El Brahmi", "coordinates": [36.604882, 8.885523]},    # lat, lon
]

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Extract lat/lon
    lats = [p["coordinates"][0] for p in ppi_points]
    lons = [p["coordinates"][1] for p in ppi_points]

    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    print(f"Grid bounding box: lat {min_lat}-{max_lat}, lon {min_lon}-{max_lon}")

    # Connect to database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        print("Database connection error:", e)
        exit()

    features = []

    lat = min_lat
    while lat < max_lat:
        lon = min_lon
        while lon < max_lon:
            # Grid cell center
            center_lat = lat + GRID_SIZE / 2
            center_lon = lon + GRID_SIZE / 2
            geoh = geohash2.encode(center_lat, center_lon, precision=7)

            # Grid cell polygon
            poly = box(lon, lat, lon + GRID_SIZE, lat + GRID_SIZE)

            # Insert into PostGIS table
            cur.execute("""
                INSERT INTO grid_100m (geohash, lat, lon, geom)
                VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
                ON CONFLICT (geohash) DO NOTHING
            """, (geoh, center_lat, center_lon, poly.wkt))

            # Prepare GeoJSON feature
            feature = {
                "type": "Feature",
                "geometry": mapping(poly),
                "properties": {
                    "geohash": geoh,
                    "center_lat": center_lat,
                    "center_lon": center_lon
                }
            }
            features.append(feature)

            lon += GRID_SIZE
        lat += GRID_SIZE

    # Commit to database
    conn.commit()
    conn.close()

    # Save to GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(GEOJSON_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Grid created successfully: {len(features)} cells")
    print(f"GeoJSON output saved to: {GEOJSON_FILE}")
