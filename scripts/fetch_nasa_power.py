import requests
import json
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
# -----------------------------
# NASA POWER data fetch function
# -----------------------------
def fetch_data(lat, lon, days=3):
    """
    Fetch daily NASA POWER data for a given latitude/longitude.
    Automatically uses last available days to avoid 422 errors.
    Replaces -999 values with None.
    """
    # Use last available data (usually yesterday or earlier)
    end_date = date.today() - timedelta(days=3)  # 3 days back to be safe
    start_date = end_date - timedelta(days=days-1)

    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        f"?parameters=T2M_MAX,T2M_MIN,PRECTOTCORR,ALLSKY_SFC_SW_DWN,RH2M,WS2M"
        f"&community=AG"
        f"&start={start_date.strftime('%Y%m%d')}"
        f"&end={end_date.strftime('%Y%m%d')}"
        f"&latitude={lat}&longitude={lon}&format=JSON"
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request error for lat={lat}, lon={lon}: {e}")
        return []

    data = response.json().get("properties", {}).get("parameter", {})
    if not data or "T2M_MIN" not in data:
        print(f"No data returned for lat={lat}, lon={lon}")
        return []

    results = []
    for date_key in data["T2M_MIN"].keys():
        # Replace -999 with None
        results.append({
            "date": date_key,
            "tmin": None if data["T2M_MIN"].get(date_key) == -999.0 else data["T2M_MIN"].get(date_key),
            "tmax": None if data["T2M_MAX"].get(date_key) == -999.0 else data["T2M_MAX"].get(date_key),
            "radiation": None if data["ALLSKY_SFC_SW_DWN"].get(date_key) == -999.0 else data["ALLSKY_SFC_SW_DWN"].get(date_key),
            "rain": None if data["PRECTOTCORR"].get(date_key) == -999.0 else data["PRECTOTCORR"].get(date_key),
            "rh": None if data["RH2M"].get(date_key) == -999.0 else data["RH2M"].get(date_key),
            "wind": None if data["WS2M"].get(date_key) == -999.0 else data["WS2M"].get(date_key),
        })

    return results

# -----------------------------
# Load grid points from GeoJSON
# -----------------------------
def load_grid_points(geojson_path):
    """Load center coordinates from GeoJSON grid file."""
    try:
        with open(geojson_path, 'r') as f:
            geojson_data = json.load(f)
        
        points = []
        for feature in geojson_data.get('features', []):
            properties = feature.get('properties', {})
            if 'center_lat' in properties and 'center_lon' in properties:
                points.append({
                    "name": f"grid_{properties.get('geohash', 'unknown')}",
                    "coordinates": [properties['center_lat'], properties['center_lon']],
                    "geohash": properties.get('geohash'),
                    "bbox": feature.get('geometry', {}).get('coordinates', [])
                })
        
        print(f"Loaded {len(points)} grid points from {geojson_path}")
        return points
    except FileNotFoundError:
        print(f"Error: File {geojson_path} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {geojson_path}")
        return []

# -----------------------------
# Save results to JSON
# -----------------------------
def save_results(results, output_path):
    """Save all fetched weather data to a JSON file."""
    try:
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {output_path}")
    except Exception as e:
        print(f"Error saving results: {e}")

# -----------------------------
# Main execution
# -----------------------------
if __name__ == "__main__":
    # Load grid points from GeoJSON
    geojson_file = "output/grid_100m.geojson"
    grid_points = load_grid_points(geojson_file)
    
    if not grid_points:
        print("No grid points loaded. Exiting.")
        exit(1)
    
    # Fetch data for all grid points
    all_results = []
    successful_fetches = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_data, point) for point in grid_points]
    
    for i, point in enumerate(grid_points):
        lat, lon = point["coordinates"]
        print(f"\nFetching data for {point['name']} ({i+1}/{len(grid_points)}) - lat: {lat:.6f}, lon: {lon:.6f}")
        
        daily_data = fetch_data(lat, lon, days=7)
        
        if daily_data:
            successful_fetches += 1
            result_entry = {
                "point_name": point["name"],
                "geohash": point.get("geohash"),
                "latitude": lat,
                "longitude": lon,
                "bbox": point.get("bbox"),
                "weather_data": daily_data
            }
            all_results.append(result_entry)
            
            # Print summary for this point
            print(f"  ✓ Success - {len(daily_data)} days fetched")
            
            # Optional: Print first day's data as sample
            if daily_data:
                sample = daily_data[0]
                print(f"  Sample: {sample['date']} - Tmin: {sample['tmin']:.1f}°C, Tmax: {sample['tmax']:.1f}°C, Rain: {sample['rain']}mm")
        else:
            print(f"  ✗ No data fetched")
    
    # Save all results to a JSON file
    output_file = "output/weather_data_all_grid_points.json"
    save_results({
        "metadata": {
            "total_grid_points": len(grid_points),
            "successful_fetches": successful_fetches,
            "failed_fetches": len(grid_points) - successful_fetches,
            "fetch_date": date.today().isoformat(),
            "days_requested": 7,
            "data_end_date": (date.today() - timedelta(days=3)).isoformat()
        },
        "results": all_results
    }, output_file)
    
    print(f"\n{'='*50}")
    print(f"SUMMARY:")
    print(f"Total grid points: {len(grid_points)}")
    print(f"Successful fetches: {successful_fetches}")
    print(f"Failed fetches: {len(grid_points) - successful_fetches}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*50}")