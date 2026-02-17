#!/usr/bin/env python3
"""
compute_et0.py
FAO-56 Penman-Monteith reference evapotranspiration (ET0) calculation
with Excel export for all grid points
"""

import math
import json
import pandas as pd
from datetime import datetime
import os

ALTITUDE_DEFAULT = 143  # Jendouba altitude (m)

def saturation_vapor_pressure(T):
    """Calculate saturation vapor pressure (kPa)"""
    if T is None:
        return None
    return 0.6108 * math.exp((17.27 * T) / (T + 237.3))

def delta_svp(T):
    """Calculate slope of saturation vapor pressure curve (kPa/°C)"""
    if T is None:
        return None
    return (4098 * saturation_vapor_pressure(T)) / ((T + 237.3) ** 2)

def psychrometric_constant(alt):
    """Calculate psychrometric constant (kPa/°C)"""
    P = 101.3 * ((293 - 0.0065 * alt) / 293) ** 5.26
    return 0.000665 * P

def compute_et0(tmin, tmax, solar_rad, rh, wind, altitude=ALTITUDE_DEFAULT):
    """
    Calculate reference evapotranspiration (ET0) using FAO-56 Penman-Monteith
    """
    # Check for missing values
    if any(x is None for x in [tmin, tmax, solar_rad, rh, wind]):
        return None
    
    # Input validation
    if solar_rad < 0:
        return None
    
    tmean = (tmin + tmax) / 2
    
    # Vapor pressure
    es = saturation_vapor_pressure(tmean)
    ea = es * (rh / 100)
    
    # Slope of saturation vapor pressure curve
    delta = delta_svp(tmean)
    
    # Psychrometric constant
    gamma = psychrometric_constant(altitude)
    
    # Radiation terms
    Rn = solar_rad  # MJ/m²/day
    G = 0
    
    # FAO-56 Penman-Monteith equation
    try:
        numerator1 = 0.408 * delta * (Rn - G)
        numerator2 = gamma * (900 / (tmean + 273)) * wind * (es - ea)
        denominator = delta + gamma * (1 + 0.34 * wind)
        
        et0 = (numerator1 + numerator2) / denominator
        
        # ET0 cannot be negative
        return round(max(et0, 0), 2)
    except Exception:
        return None

def process_json_file(json_path, output_excel=None):
    """
    Lit le fichier JSON, calcule ET0 pour tous les points
    """
    print(f"📖 Lecture du fichier: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    results = []
    total_points = len(data.get('results', []))
    valid_count = 0
    
    for i, point in enumerate(data.get('results', [])):
        geohash = point.get('geohash')
        lat = point.get('latitude')
        lon = point.get('longitude')
        
        weather_data = point.get('weather_data', [])
        
        # Prendre le PREMIER jour avec des données valides
        selected_day = None
        for day in weather_data:
            if (day.get('tmin') is not None and 
                day.get('tmax') is not None and 
                day.get('radiation') is not None and 
                day.get('rh') is not None and 
                day.get('wind') is not None):
                selected_day = day
                break
        
        if selected_day:
            tmin = selected_day.get('tmin')
            tmax = selected_day.get('tmax')
            radiation = selected_day.get('radiation')
            rh = selected_day.get('rh')
            wind = selected_day.get('wind')
            
            # Calculer ET0
            et0 = compute_et0(tmin, tmax, radiation, rh, wind)
            
            if et0 is not None:
                valid_count += 1
            
            results.append({
                'geohash': str(geohash) if geohash else '',
                'latitude': lat,
                'longitude': lon,
                'date': selected_day.get('date'),
                'tmin': tmin,
                'tmax': tmax,
                'radiation': radiation,
                'rh': rh,
                'wind': wind,
                'et0': et0
            })
        
        # Progression
        if (i+1) % 500 == 0:
            print(f"   Progression: {i+1}/{total_points} points...")
    
    # Créer DataFrame
    df = pd.DataFrame(results)
    
    print(f"\n✅ Calcul terminé!")
    print(f"   Total points: {len(results)}")
    print(f"   ET0 valides: {valid_count}/{len(results)}")
    
    return df

def export_to_excel(df, output_path=None):
    """
    Exporte le DataFrame vers un fichier Excel
    """
    if output_path is None:
        # Créer un nom de fichier avec timestamp
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'output/et0_calculations_{date_str}.xlsx'
    
    # Créer le dossier output s'il n'existe pas
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    # Exporter vers Excel
    df.to_excel(output_path, index=False, sheet_name='ET0_Calculations')
    
    print(f"📁 Fichier Excel: {output_path}")
    return output_path

def export_to_desktop(df, filename='et0_all_grid_points.xlsx'):
    """
    Exporte le DataFrame vers le Bureau
    """
    desktop = r'C:\Users\Lenovo\Desktop'
    
    # Vérifier si le fichier existe déjà
    output_path = os.path.join(desktop, filename)
    counter = 1
    while os.path.exists(output_path):
        name, ext = os.path.splitext(filename)
        output_path = os.path.join(desktop, f"{name}_{counter}{ext}")
        counter += 1
    
    df.to_excel(output_path, index=False)
    print(f"✅ Bureau: {output_path}")
    return output_path

def test_single_point():
    """Test avec un point spécifique"""
    print("\n🧪 TEST: Calcul ET0 avec valeurs de test")
    
    # Test avec des valeurs réalistes
    et0 = compute_et0(15.5, 28.3, 22.5, 65, 2.1)
    print(f"   ET0 test (15.5°C, 28.3°C, 22.5 MJ/m², 65%, 2.1 m/s) = {et0} mm/jour")
    
    # Test avec les premières données du JSON
    try:
        json_file = "output/weather_data_all_grid_points.json"
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            if data.get('results') and len(data['results']) > 0:
                first_point = data['results'][0]
                if first_point.get('weather_data'):
                    # Chercher le premier jour valide
                    for day in first_point['weather_data']:
                        if (day.get('tmin') is not None and 
                            day.get('tmax') is not None and 
                            day.get('radiation') is not None and 
                            day.get('rh') is not None and 
                            day.get('wind') is not None):
                            
                            tmin = day.get('tmin')
                            tmax = day.get('tmax')
                            radiation = day.get('radiation')
                            rh = day.get('rh')
                            wind = day.get('wind')
                            
                            print(f"\n📊 Premier point du fichier:")
                            print(f"   geohash: {first_point.get('geohash')}")
                            print(f"   date: {day.get('date')}")
                            print(f"   tmin: {tmin}°C")
                            print(f"   tmax: {tmax}°C")
                            print(f"   radiation: {radiation} MJ/m²")
                            print(f"   rh: {rh}%")
                            print(f"   wind: {wind} m/s")
                            
                            et0 = compute_et0(tmin, tmax, radiation, rh, wind)
                            print(f"   ET0 calculé: {et0} mm/jour")
                            break
    except Exception as e:
        print(f"   ⚠️ Erreur test: {e}")

def main():
    """Fonction principale"""
    print("="*60)
    print("🌍 CALCUL ET0 - FAO-56 Penman-Monteith")
    print("="*60)
    
    # 1. Tester avec un point
    test_single_point()
    
    print("\n" + "="*60)
    
    # 2. Traiter tout le fichier
    json_file = "output/weather_data_all_grid_points.json"
    
    if not os.path.exists(json_file):
        print(f"❌ Fichier non trouvé: {json_file}")
        return
    
    # Traiter le fichier JSON
    df = process_json_file(json_file)
    
    if len(df) == 0:
        print("❌ Aucune donnée traitée")
        return
    
    # 3. Exporter vers le dossier output
    output_file = export_to_excel(df)
    
    # 4. Exporter vers le Bureau
    desktop_file = export_to_desktop(df, 'et0_all_grid_points.xlsx')
    
    # 5. Afficher les statistiques
    valid_et0 = df['et0'].notna().sum()
    if valid_et0 > 0:
        print(f"\n📈 Statistiques ET0:")
        print(f"   Points valides: {valid_et0}/{len(df)}")
        print(f"   Moyenne: {df['et0'].mean():.2f} mm/jour")
        print(f"   Min: {df['et0'].min():.2f} mm/jour")
        print(f"   Max: {df['et0'].max():.2f} mm/jour")
    
    print(f"\n✅ Export terminé!")
    print(f"   - Dossier output: {output_file}")
    print(f"   - Bureau: {desktop_file}")

if __name__ == "__main__":
    main()