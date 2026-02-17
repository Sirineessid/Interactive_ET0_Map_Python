#!/usr/bin/env python3
"""
make_map.py
Generate interactive map with gridded climate data and ET0 coloring
Usage: python make_map.py [--date YYYY-MM-DD] [--param et0]
"""

import os
import sys
import json
import argparse
import folium
from folium import plugins
import psycopg2
from psycopg2.extras import DictCursor
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import branca.colormap as cm
import math

load_dotenv()

DB_CONFIG = {

}

# Color schemes for different parameters
COLOR_SCHEMES = {
    'et0': {
        'title': 'ET0 (mm/day)',
        'min': 0,
        'max': 10,
        'suffix': ' mm',
        'colors': ['#0000FF', '#0066FF', '#00CCFF', '#00FFCC', '#00FF00', '#99FF00', '#FFFF00', '#FF9900', '#FF3300', '#FF0000']
    },
    'tmin': {
        'title': 'Min Temperature (°C)',
        'min': 0,
        'max': 25,
        'suffix': '°C',
        'colors': ['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6', '#4292c6', '#2171b5', '#08519c', '#08306b']
    },
    'tmax': {
        'title': 'Max Temperature (°C)',
        'min': 15,
        'max': 40,
        'suffix': '°C',
        'colors': ['#fff5f0', '#fee0d2', '#fcbba1', '#fc9272', '#fb6a4a', '#ef3b2c', '#cb181d', '#a50f15', '#67000d']
    },
    'rain': {
        'title': 'Precipitation (mm)',
        'min': 0,
        'max': 50,
        'suffix': ' mm',
        'colors': ['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6', '#4292c6', '#2171b5', '#08519c', '#08306b']
    },
    'rh': {
        'title': 'Relative Humidity (%)',
        'min': 20,
        'max': 100,
        'suffix': '%',
        'colors': ['#edf8e9', '#c7e9c0', '#a1d99b', '#74c476', '#41ab5d', '#238b45', '#006d2c', '#00441b']
    },
    'wind': {
        'title': 'Wind Speed (m/s)',
        'min': 0,
        'max': 10,
        'suffix': ' m/s',
        'colors': ['#f2f0f7', '#dadaeb', '#bcbddc', '#9e9ac8', '#807dba', '#6a51a3', '#54278f', '#3f007d']
    }
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=DictCursor)
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None

def get_available_dates(conn):
    """Get all available dates from climate_daily"""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT date FROM climate_daily WHERE et0 IS NOT NULL ORDER BY date")
        return [row[0] for row in cur.fetchall()]

def get_climate_data_from_db(conn, target_date=None):
    """Get climate data for mapping from climate_daily table - VERSION CORRIGÉE"""
    
    with conn.cursor() as cur:
        # Étape 1: Récupérer toutes les dates qui ont des données NON NULLES pour et0
        cur.execute("""
            SELECT DISTINCT date 
            FROM climate_daily 
            WHERE et0 IS NOT NULL 
            ORDER BY date
        """)
        available_dates = [row[0] for row in cur.fetchall()]
        
        if not available_dates:
            print("❌ Aucune date avec des données valides trouvée dans climate_daily")
            return []
        
        print(f"📅 Dates avec données valides: {available_dates[0]} → {available_dates[-1]}")
        
        # Étape 2: Déterminer la date à utiliser
        if target_date is None:
            target_date = available_dates[-1]
            print(f"📅 Utilisation de la dernière date avec données: {target_date}")
        elif target_date not in available_dates:
            print(f"⚠️ Date {target_date} non disponible ou sans données")
            print(f"📅 Utilisation de {available_dates[-1]} à la place")
            target_date = available_dates[-1]
        
        # Étape 3: Récupérer les données pour cette date
        cur.execute("""
            SELECT 
                g.geohash,
                g.lat,
                g.lon,
                ST_AsGeoJSON(g.geom) as geom,
                d.date,
                d.tmin,
                d.tmax,
                d.rain,
                d.rh,
                d.wind,
                d.et0
            FROM climate_daily d
            JOIN grid_100m g ON d.geohash = g.geohash
            WHERE d.date = %s AND d.et0 IS NOT NULL
            ORDER BY g.geohash
        """, (target_date,))
        
        results = cur.fetchall()
        
        if results:
            print(f"✅ {len(results)} points avec données trouvés pour le {target_date}")
            
            # Vérifier les valeurs du premier point
            first = results[0]
            print(f"\n🔍 Premier point avec données:")
            print(f"   geohash: {first['geohash']}")
            print(f"   date: {first['date']}")
            print(f"   tmin: {first['tmin']}°C")
            print(f"   tmax: {first['tmax']}°C")
            print(f"   et0: {first['et0']} mm")
            print(f"   rain: {first['rain']} mm")
            
            # Statistiques rapides
            et0_values = [r['et0'] for r in results if r['et0'] is not None]
            if et0_values:
                print(f"\n📊 Statistiques ET0 pour cette date:")
                print(f"   Min: {min(et0_values):.2f} mm")
                print(f"   Max: {max(et0_values):.2f} mm")
                print(f"   Moyenne: {sum(et0_values)/len(et0_values):.2f} mm")
        else:
            print(f"⚠️ Aucun point avec données pour le {target_date}")
        
        return results

def get_climate_data_from_json(json_path, target_date=None):
    """Get climate data from JSON file"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Obtenir toutes les dates disponibles dans le JSON
        all_dates = set()
        for result in data.get('results', []):
            for day in result.get('weather_data', []):
                if day.get('date') and day.get('et0') is not None:
                    all_dates.add(day['date'])
        
        available_dates = sorted(list(all_dates))
        if not available_dates:
            print("❌ Aucune date avec données valides dans le JSON")
            return []
            
        print(f"📅 Dates disponibles dans JSON: {available_dates[0]} → {available_dates[-1]}")
        
        # Déterminer la date cible
        target_str = None
        if target_date:
            target_str = target_date.strftime('%Y%m%d')
            if target_str not in available_dates:
                print(f"⚠️ Date {target_str} non disponible dans JSON")
                target_str = available_dates[-1]
                print(f"📅 Utilisation de la dernière date: {target_str}")
        else:
            target_str = available_dates[-1]
        
        print(f"📅 Chargement des données pour le {target_str}")
        
        climate_data = []
        for result in data.get('results', []):
            if result.get('weather_data'):
                weather_data = result.get('weather_data', [])
                # Chercher le jour correspondant à la date cible
                selected_day = next((d for d in weather_data if d.get('date') == target_str and d.get('et0') is not None), None)
                
                if selected_day:
                    climate_data.append({
                        'geohash': result.get('geohash', ''),
                        'lat': float(result.get('latitude', 0)),
                        'lon': float(result.get('longitude', 0)),
                        'geom': None,
                        'date': selected_day.get('date'),
                        'tmin': selected_day.get('tmin'),
                        'tmax': selected_day.get('tmax'),
                        'rain': selected_day.get('rain'),
                        'rh': selected_day.get('rh'),
                        'wind': selected_day.get('wind'),
                        'et0': selected_day.get('et0')
                    })
        
        print(f"✅ {len(climate_data)} points chargés depuis le JSON")
        return climate_data
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement du JSON: {e}")
        return []

def get_color(value, param='et0'):
    """Get color based on parameter value"""
    scheme = COLOR_SCHEMES.get(param, COLOR_SCHEMES['et0'])
    
    if value is None:
        return '#808080'  # Gray for no data
    
    # Normalize value
    vmin = scheme['min']
    vmax = scheme['max']
    
    if vmax == vmin:
        normalized = 0.5
    else:
        normalized = max(0, min(1, (value - vmin) / (vmax - vmin)))
    
    colors = scheme['colors']
    index = int(normalized * (len(colors) - 1))
    return colors[index]

def create_square_grid(center_lat, center_lon, size_m=100):
    """Create a square polygon around center point"""
    lat_offset = size_m / 2 / 111111.0
    lon_offset = size_m / 2 / (111111.0 * math.cos(math.radians(center_lat)))
    
    coords = [
        [center_lon - lon_offset, center_lat - lat_offset],
        [center_lon + lon_offset, center_lat - lat_offset],
        [center_lon + lon_offset, center_lat + lat_offset],
        [center_lon - lon_offset, center_lat + lat_offset],
        [center_lon - lon_offset, center_lat - lat_offset]
    ]
    
    return {
        "type": "Polygon",
        "coordinates": [coords]
    }

def create_popup_content(row):
    """Create HTML popup content"""
    # Format date
    if isinstance(row['date'], date):
        date_str = row['date'].strftime('%Y-%m-%d')
    else:
        date_str = str(row['date'])
    
    # Format values with proper handling of None
    tmin_val = f"{row['tmin']:.1f}°C" if row['tmin'] is not None else 'N/A'
    tmax_val = f"{row['tmax']:.1f}°C" if row['tmax'] is not None else 'N/A'
    et0_val = f"{row['et0']:.2f} mm" if row['et0'] is not None else 'N/A'
    rain_val = f"{row['rain']:.1f} mm" if row['rain'] is not None else 'N/A'
    rh_val = f"{row['rh']:.0f}%" if row['rh'] is not None else 'N/A'
    wind_val = f"{row['wind']:.1f} m/s" if row['wind'] is not None else 'N/A'
    
    html = f"""
    <div style="font-family: Arial; min-width: 240px; padding: 10px; background-color: white; border-radius: 5px;">
        <h4 style="margin:0 0 10px 0; color:#2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 5px;">
            🧩 Grid Cell: {row['geohash']}
        </h4>
        <table style="width:100%; border-collapse: collapse;">
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 5px;"><strong>📅 Date:</strong></td>
                <td style="padding: 5px; text-align:right; font-weight: bold;">{date_str}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>🌡️ Tmin:</strong></td>
                <td style="padding: 5px; text-align:right; color: #2980b9;">{tmin_val}</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 5px;"><strong>🌡️ Tmax:</strong></td>
                <td style="padding: 5px; text-align:right; color: #c0392b;">{tmax_val}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>💧 ET0:</strong></td>
                <td style="padding: 5px; text-align:right; color: #27ae60; font-weight: bold;">{et0_val}</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 5px;"><strong>☔ Rain:</strong></td>
                <td style="padding: 5px; text-align:right; color: #3498db;">{rain_val}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>💨 RH:</strong></td>
                <td style="padding: 5px; text-align:right; color: #8e44ad;">{rh_val}</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 5px;"><strong>🌀 Wind:</strong></td>
                <td style="padding: 5px; text-align:right; color: #7f8c8d;">{wind_val}</td>
            </tr>
        </table>
        <p style="margin:10px 0 0 0; font-size:0.8em; color:#7f8c8d; text-align:center; border-top: 1px solid #ecf0f1; padding-top: 5px;">
            Click for more details
        </p>
    </div>
    """
    return html

def create_map(climate_data, param='et0', output_file='output/climate_map.html'):
    """Create interactive Folium map with square grids"""
    
    if not climate_data:
        print("❌ Aucune donnée à afficher sur la carte")
        return None
    
    # Center map on average coordinates
    center_lat = sum(float(row['lat']) for row in climate_data) / len(climate_data)
    center_lon = sum(float(row['lon']) for row in climate_data) / len(climate_data)
    
    print(f"📍 Centre de la carte: {center_lat:.4f}, {center_lon:.4f}")
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='OpenStreetMap',
        control_scale=True
    )
    
    # Add plugins
    plugins.Fullscreen().add_to(m)
    plugins.MiniMap(toggle_display=True, position='bottomright').add_to(m)
    plugins.MousePosition().add_to(m)
    plugins.MeasureControl(position='topleft').add_to(m)
    
    # Create color scale
    scheme = COLOR_SCHEMES.get(param, COLOR_SCHEMES['et0'])
    
    colormap = cm.LinearColormap(
        colors=scheme['colors'],
        vmin=scheme['min'],
        vmax=scheme['max'],
        caption=scheme['title']
    )
    m.add_child(colormap)
    
    
    
    # Add grid cells
    points_added = 0
    values_count = 0
    
    for row in climate_data:
        value = row[param]
        if value is not None:
            values_count += 1
        
        color = get_color(value, param)
        
        # Create square geometry
        geom = create_square_grid(float(row['lat']), float(row['lon']))
        
        # Create popup and tooltip
        popup = folium.Popup(create_popup_content(row), max_width=300)
        tooltip = f"{row['geohash']}"
        if value is not None:
            tooltip += f": {value:.2f}{scheme['suffix']}"
        
        # Add square to map
        folium.GeoJson(
            geom,
            style_function=lambda x, c=color: {
                'fillColor': c,
                'color': 'black',
                'weight': 0.5,
                'fillOpacity': 0.2
            },
            highlight_function=lambda x: {
                'weight': 2,
                'color': 'black',
                'fillOpacity': 0.5
            },
            popup=popup,
            tooltip=tooltip
        ).add_to(m)
        
        points_added += 1
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Add draw tools
    plugins.Draw(export=True).add_to(m)
    
    # Save map
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    m.save(output_file)
    
    print(f"✅ {points_added} carrés affichés sur la carte")
    print(f"   {values_count}/{points_added} avec valeurs {param}")
    print(f"🗺️ Map saved to {output_file}")
    return m

def main():
    parser = argparse.ArgumentParser(description='Generate climate map with ET0 coloring')
    parser.add_argument('--date', type=str, help='Date for data (YYYY-MM-DD)')
    parser.add_argument('--param', type=str, default='et0',
                       choices=['et0', 'tmin', 'tmax', 'rain', 'rh', 'wind'],
                       help='Parameter to color by')
    parser.add_argument('--output', type=str, default='output/climate_map.html',
                       help='Output HTML file')
    parser.add_argument('--use-json', type=str,
                       help='Use JSON file instead of database')
    parser.add_argument('--list-dates', action='store_true',
                       help='List available dates in database')
    
    args = parser.parse_args()
    
    print("="*60)
    print("🌍 GÉNÉRATION DE CARTE CLIMATIQUE")
    print("="*60)
    
    # Parse date
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        print(f"📅 Date demandée: {target_date}")
    
    # Get climate data
    climate_data = []
    
    if args.use_json:
        # JSON mode
        json_path = args.use_json
        if not os.path.exists(json_path):
            json_path = os.path.join('output', args.use_json)
        print(f"📁 Source: JSON ({json_path})")
        climate_data = get_climate_data_from_json(json_path, target_date)
    
    elif args.list_dates:
        # List dates mode
        conn = get_db_connection()
        if conn:
            dates = get_available_dates(conn)
            print("\n📅 Dates disponibles dans climate_daily (avec données):")
            for d in dates:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM climate_daily WHERE date = %s AND et0 IS NOT NULL", (d,))
                    count = cur.fetchone()[0]
                print(f"   - {d}: {count} enregistrements")
            conn.close()
        return
    
    else:
        # Database mode
        print("📁 Source: Base de données")
        conn = get_db_connection()
        if not conn:
            print("❌ Impossible de se connecter à la base de données")
            print("💡 Utilisez --use-json pour utiliser le fichier JSON")
            sys.exit(1)
        
        try:
            climate_data = get_climate_data_from_db(conn, target_date)
        finally:
            conn.close()
        
        # Fallback to JSON if no data in DB
        if not climate_data:
            print("\n⚠️ Aucune donnée en base, tentative avec le JSON par défaut...")
            default_json = os.path.join('output', 'weather_data_all_grid_points.json')
            if os.path.exists(default_json):
                climate_data = get_climate_data_from_json(default_json, target_date)
    
    if not climate_data:
        print("❌ Aucune donnée trouvée!")
        print("\n👉 Vérifiez que:")
        print("   1. La date existe: python -m scripts.make_map --list-dates")
        print("   2. Le fichier JSON existe: output/weather_data_all_grid_points.json")
        print("\n💡 Exemple: python -m scripts.make_map --date 2026-02-07 --param et0")
        sys.exit(1)
    
    # Statistics
    values = [row[args.param] for row in climate_data if row[args.param] is not None]
    print(f"\n📊 Statistiques {args.param}:")
    print(f"   Points total: {len(climate_data)}")
    print(f"   Points avec valeurs: {len(values)}")
    
    if values:
        print(f"   Min: {min(values):.2f}")
        print(f"   Max: {max(values):.2f}")
        print(f"   Moyenne: {sum(values)/len(values):.2f}")
    else:
        print(f"   ⚠️ Aucune valeur valide pour {args.param}")
    
    # Create map
    create_map(climate_data, args.param, args.output)
    
    print(f"\n✅ Carte générée avec succès!")
    print(f"📁 Fichier: {args.output}")
    print(f"👉 Ouvrez ce fichier dans votre navigateur")

if __name__ == "__main__":
    main()
