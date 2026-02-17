#!/usr/bin/env python3
"""
aggregate_7days.py
Calculate 7-day rolling averages from climate_daily table
Usage: python aggregate_7days.py [--date YYYY-MM-DD] [--auto]
"""

import sys
import argparse
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import os

load_dotenv()

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

def check_data_availability(conn):
    """Vérifie la disponibilité des données dans climate_daily"""
    with conn.cursor() as cur:
        # Compter les enregistrements
        cur.execute("SELECT COUNT(*) FROM climate_daily")
        total_records = cur.fetchone()[0]
        
        if total_records == 0:
            print("❌ Aucune donnée dans climate_daily")
            print("👉 Veuillez d'abord exécuter: python -m scripts.ingest_climate")
            return False
        
        # Obtenir la plage de dates
        cur.execute("SELECT MIN(date), MAX(date) FROM climate_daily")
        min_date, max_date = cur.fetchone()
        
        # Compter les geohash distincts
        cur.execute("SELECT COUNT(DISTINCT geohash) FROM climate_daily")
        geohash_count = cur.fetchone()[0]
        
        print(f"\n📊 ÉTAT DE climate_daily:")
        print(f"   Total enregistrements: {total_records}")
        print(f"   Points de grille: {geohash_count}")
        print(f"   Période disponible: {min_date} → {max_date}")
        
        return True

def aggregate_7days(conn, target_date=None, min_days_required=3):
    """
    Calculate 7-day averages ending on target_date
    min_days_required: nombre minimum de jours requis pour calculer une moyenne (défaut: 3)
    """
    with conn.cursor() as cur:
        # Si pas de date spécifiée, prendre la dernière date disponible
        if target_date is None:
            cur.execute("SELECT MAX(date) FROM climate_daily")
            max_date = cur.fetchone()[0]
            if max_date:
                target_date = max_date
                print(f"\n📅 Utilisation de la dernière date disponible: {target_date}")
            else:
                print("❌ Aucune donnée dans climate_daily")
                return 0
        
        start_date = target_date - timedelta(days=6)
        
        print(f"📊 Agrégation sur 7 jours: {start_date} → {target_date}")
        print(f"   Seuil minimum: {min_days_required} jours")
        
        # Vérifier les données dans cette période
        cur.execute("""
            SELECT 
                COUNT(DISTINCT geohash) as points_count,
                COUNT(*) as records_count
            FROM climate_daily 
            WHERE date BETWEEN %s AND %s
        """, (start_date, target_date))
        
        points_count, records_count = cur.fetchone()
        print(f"   Points avec données: {points_count}")
        print(f"   Enregistrements: {records_count}")
        
        if points_count == 0:
            print("⚠️ Aucune donnée dans cette période")
            return 0
        
        # Récupérer tous les geohash qui ont des données dans la période
        cur.execute("""
            SELECT DISTINCT geohash 
            FROM climate_daily 
            WHERE date BETWEEN %s AND %s
            ORDER BY geohash
        """, (start_date, target_date))
        
        grid_points = [row[0] for row in cur.fetchall()]
        print(f"   {len(grid_points)} geohash à traiter")
        
        weekly_data = []
        points_insuffisants = 0
        
        # Pour chaque point, calculer les moyennes sur 7 jours
        for i, geohash in enumerate(grid_points):
            cur.execute("""
                SELECT 
                    COUNT(*) as days_count,
                    ROUND(AVG(tmin)::numeric, 2) as avg_tmin,
                    ROUND(AVG(tmax)::numeric, 2) as avg_tmax,
                    ROUND(AVG(radiation)::numeric, 2) as avg_radiation,
                    ROUND(SUM(rain)::numeric, 2) as total_rain,
                    ROUND(AVG(rh)::numeric, 2) as avg_rh,
                    ROUND(AVG(wind)::numeric, 2) as avg_wind,
                    ROUND(AVG(et0)::numeric, 2) as avg_et0
                FROM climate_daily
                WHERE geohash = %s 
                    AND date BETWEEN %s AND %s
                    AND tmin IS NOT NULL
            """, (geohash, start_date, target_date))
            
            row = cur.fetchone()
            days_count = row[0]
            
            # Afficher la progression
            if (i + 1) % 100 == 0:
                print(f"      Progression: {i+1}/{len(grid_points)} points...")
            
            # 🔥 CORRECTION: Utiliser min_days_required au lieu de 5
            if days_count >= min_days_required:
                weekly_data.append((
                    geohash,
                    start_date,
                    target_date,
                    row[1],  # avg_tmin
                    row[2],  # avg_tmax
                    row[3],  # avg_radiation
                    row[4],  # total_rain
                    row[5],  # avg_rh
                    row[6],  # avg_wind
                    row[7]   # avg_et0
                ))
            else:
                points_insuffisants += 1
        
        print(f"   Points avec ≥{min_days_required} jours: {len(weekly_data)}/{len(grid_points)}")
        if points_insuffisants > 0:
            print(f"   Points avec moins de {min_days_required} jours: {points_insuffisants}")
        
        # Insert weekly summaries
        if weekly_data:
            execute_values(cur, """
                INSERT INTO climate_7days 
                (geohash, start_date, end_date, avg_tmin, avg_tmax, 
                 avg_radiation, total_rain, avg_rh, avg_wind, avg_et0)
                VALUES %s
                ON CONFLICT (geohash, end_date) DO UPDATE
                SET start_date = EXCLUDED.start_date,
                    avg_tmin = EXCLUDED.avg_tmin,
                    avg_tmax = EXCLUDED.avg_tmax,
                    avg_radiation = EXCLUDED.avg_radiation,
                    total_rain = EXCLUDED.total_rain,
                    avg_rh = EXCLUDED.avg_rh,
                    avg_wind = EXCLUDED.avg_wind,
                    avg_et0 = EXCLUDED.avg_et0,
                    updated_at = NOW()
            """, weekly_data)
            
            conn.commit()
            print(f"✅ Aggregated {len(weekly_data)} weekly records")
        else:
            print(f"⚠️ No weekly data to aggregate (need at least {min_days_required} days per point)")
        
        return len(weekly_data)

def verify_aggregation(conn):
    """Vérifie les résultats dans climate_7days"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM climate_7days")
        count = cur.fetchone()[0]
        
        if count > 0:
            cur.execute("""
                SELECT end_date, COUNT(*) 
                FROM climate_7days 
                GROUP BY end_date 
                ORDER BY end_date DESC
                LIMIT 5
            """)
            results = cur.fetchall()
            
            print(f"\n📊 RÉSULTATS DE L'AGRÉGATION:")
            print(f"   Total enregistrements: {count}")
            print("   Dernières périodes:")
            for date, cnt in results:
                print(f"      {date}: {cnt} points")
            
            # Afficher un échantillon
            cur.execute("""
                SELECT geohash, end_date, avg_tmin, avg_tmax, avg_et0 
                FROM climate_7days 
                LIMIT 3
            """)
            samples = cur.fetchall()
            if samples:
                print("\n   Échantillon:")
                for s in samples:
                    print(f"      {s[0]} - {s[1]}: Tmin={s[2]}°C, Tmax={s[3]}°C, ET0={s[4]}mm")
        else:
            print("\n⚠️ Aucun résultat dans climate_7days")
        
        return count

def main():
    parser = argparse.ArgumentParser(description='Aggregate 7-day climate data')
    parser.add_argument('--date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--backfill', type=int, default=0,
                       help='Backfill N days')
    parser.add_argument('--auto', action='store_true',
                       help='Use the latest available date automatically')
    parser.add_argument('--min-days', type=int, default=3,
                       help='Minimum days required for aggregation (default: 3)')
    parser.add_argument('--force', action='store_true',
                       help='Force recalculation even if exists')
    parser.add_argument('--debug', action='store_true',
                       help='Show debug information')
    
    args = parser.parse_args()
    
    print("="*60)
    print("🌍 AGRÉGATION DES DONNÉES SUR 7 JOURS")
    print("="*60)
    print(f"📅 Seuil minimum: {args.min_days} jours")
    
    conn = get_db_connection()
    if not conn:
        sys.exit(1)
    
    try:
        # Vérifier la disponibilité des données
        if not check_data_availability(conn):
            sys.exit(1)
        
        # Obtenir la plage de dates disponible
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(date), MAX(date) FROM climate_daily")
            min_date, max_date = cur.fetchone()
        
        # Déterminer la ou les dates à traiter
        dates_to_process = []
        
        if args.auto:
            # Utiliser la dernière date disponible
            dates_to_process.append(max_date)
            print(f"\n📅 Mode auto: utilisation de {max_date}")
        
        elif args.date:
            # Date spécifique
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            if target_date > max_date:
                print(f"⚠️ Date {target_date} au-delà des données disponibles (max: {max_date})")
                target_date = max_date
            if target_date >= min_date:
                dates_to_process.append(target_date)
            else:
                print(f"⚠️ Date {target_date} avant les données disponibles (min: {min_date})")
        
        elif args.backfill > 0:
            # Backfill plusieurs dates
            end_date = max_date
            for i in range(min(args.backfill, 30)):  # Max 30 jours
                current_date = end_date - timedelta(days=i)
                if current_date >= min_date:
                    dates_to_process.append(current_date)
            print(f"\n📅 Backfill de {len(dates_to_process)} dates")
        
        else:
            # Par défaut: dernière date
            dates_to_process.append(max_date)
            print(f"\n📅 Utilisation de la dernière date: {max_date}")
        
        # Traiter chaque date
        total_aggregated = 0
        for i, target_date in enumerate(dates_to_process):
            print(f"\n--- Période {i+1}/{len(dates_to_process)} ---")
            count = aggregate_7days(conn, target_date, args.min_days)
            total_aggregated += count
        
        # Vérifier les résultats
        verify_aggregation(conn)
        
        print(f"\n" + "="*60)
        print(f"✅ AGRÉGATION TERMINÉE")
        print(f"   Total périodes traitées: {len(dates_to_process)}")
        print(f"   Total enregistrements: {total_aggregated}")
        print(f"   Seuil utilisé: {args.min_days} jours")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()