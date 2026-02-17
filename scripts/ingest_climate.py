#!/usr/bin/env python3
"""
ingest_climate.py
Ingest weather data from JSON file into PostgreSQL database
Usage: python ingest_climate.py [--json-file output/weather_data_all_grid_points.json]
"""

import os
import sys
import json
import argparse
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from compute_et0 import compute_et0

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "nasa_et0"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Sirine123")
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def load_json_data(json_path):
    """Load weather data from JSON file"""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        print(f"✅ Loaded JSON data from {json_path}")
        return data
    except Exception as e:
        print(f"❌ Error loading JSON: {e}")
        return None

def ingest_grid_points(conn, results):
    """Insert grid points into grid_100m table"""
    # Utiliser un dictionnaire pour éviter les doublons de geohash
    unique_grid = {}
    
    for result in results:
        geohash = result.get('geohash')
        if geohash and result.get('latitude') and result.get('longitude'):
            # Garder seulement la première occurrence
            if geohash not in unique_grid:
                # Create polygon from bbox if available
                geom = None
                bbox = result.get('bbox')
                if bbox and len(bbox) > 0 and len(bbox[0]) >= 4:
                    coords = bbox[0]
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])
                    coords_str = ', '.join([f"{c[0]} {c[1]}" for c in coords])
                    geom = f"POLYGON(({coords_str}))"
                
                unique_grid[geohash] = (
                    geohash,
                    result['latitude'],
                    result['longitude'],
                    geom
                )
    
    grid_data = list(unique_grid.values())
    
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO grid_100m (geohash, lat, lon, geom)
            VALUES %s
            ON CONFLICT (geohash) DO UPDATE
            SET lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                geom = EXCLUDED.geom,
                created_at = NOW()
        """, grid_data)
        conn.commit()
    
    print(f"✅ Ingested {len(grid_data)} grid points")
    return len(grid_data)

def ingest_climate_daily(conn, results):
    """Insert daily climate data with ET0 calculation"""
    daily_data = []
    
    for result in results:
        geohash = result.get('geohash')
        weather_data = result.get('weather_data', [])
        
        # Pour chaque jour, ajouter aux données
        for day in weather_data:
            # Calculer ET0
            et0 = compute_et0(
                day.get('tmin'),
                day.get('tmax'),
                day.get('radiation'),
                day.get('rh'),
                day.get('wind')
            )
            
            daily_data.append((
                geohash,
                day['date'],
                day.get('tmin'),
                day.get('tmax'),
                day.get('radiation'),
                day.get('rain'),
                day.get('rh'),
                day.get('wind'),
                et0
            ))
    
    if daily_data:
        # 🔥 ÉTAPE 1: Supprimer les doublons dans daily_data (même geohash + même date)
        unique_data = {}
        for record in daily_data:
            key = (record[0], record[1])  # (geohash, date)
            if key not in unique_data:
                unique_data[key] = record
        
        daily_data_unique = list(unique_data.values())
        
        print(f"   Filtrage: {len(daily_data)} → {len(daily_data_unique)} enregistrements uniques")
        
        with conn.cursor() as cur:
            # 🔥 ÉTAPE 2: Diviser en lots pour éviter les problèmes
            batch_size = 500
            total_inserted = 0
            
            for i in range(0, len(daily_data_unique), batch_size):
                batch = daily_data_unique[i:i + batch_size]
                
                # Utiliser ON CONFLICT DO NOTHING pour ignorer les doublons existants
                execute_values(cur, """
                    INSERT INTO climate_daily 
                    (geohash, date, tmin, tmax, radiation, rain, rh, wind, et0)
                    VALUES %s
                    ON CONFLICT (geohash, date) DO NOTHING
                """, batch)
                
                total_inserted += len(batch)
            
            conn.commit()
        
        print(f"✅ Ingested {total_inserted} daily records")
        return total_inserted
    
    return 0

def main():
    parser = argparse.ArgumentParser(description='Ingest climate data into database')
    parser.add_argument('--json-file', type=str, 
                       default='output/weather_data_all_grid_points.json',
                       help='Path to weather data JSON file')
    parser.add_argument('--no-grid', action='store_true',
                       help='Skip grid ingestion')
    
    args = parser.parse_args()
    
    # Load JSON data
    data = load_json_data(args.json_file)
    if not data:
        sys.exit(1)
    
    results = data.get('results', [])
    if not results:
        print("❌ No results found in JSON file")
        sys.exit(1)
    
    print(f"📊 JSON contient {len(results)} points")
    
    # Connect to database
    conn = get_db_connection()
    if not conn:
        sys.exit(1)
    
    try:
        # Ingest grid points
        if not args.no_grid:
            ingest_grid_points(conn, results)
        
        # Ingest daily climate data
        ingest_climate_daily(conn, results)
        
        print("\n" + "="*60)
        print("✅ INGESTION COMPLETED SUCCESSFULLY")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()