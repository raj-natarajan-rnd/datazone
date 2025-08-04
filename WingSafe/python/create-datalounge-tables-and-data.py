import boto3
import time

def execute_athena_query(query, description):
    """Execute an Athena query and wait for completion"""
    athena = boto3.client('athena')
    
    try:
        print(f"🔧 {description}...")
        
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': 's3://wingsafe-athena-results-dev-184838390535/setup/'
            },
            WorkGroup='WingSafe-DataAnalysis-dev'
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")
        
        # Wait for completion
        while True:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            print(f"Status: {status}")
            time.sleep(3)
        
        if status == 'SUCCEEDED':
            print(f"✅ {description} completed successfully")
            return True
        else:
            print(f"❌ {description} failed: {status}")
            if 'StateChangeReason' in response['QueryExecution']['Status']:
                print(f"Reason: {response['QueryExecution']['Status']['StateChangeReason']}")
            return False
            
    except Exception as e:
        print(f"Error in {description}: {e}")
        return False

def setup_datalounge_tables_and_data():
    """Create DataLounge Iceberg tables and insert sample data from WingSafe centralized catalog"""
    
    applications = [
        {
            'name': 'AeroNav',
            'database': 'aeronav_db',
            'bucket': 'aeronav-iceberg-data-dev-073118366505',
            'tables': [
                {
                    'name': 'navigation_waypoints',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aeronav_db.navigation_waypoints (
                        waypoint_id string,
                        waypoint_name string,
                        latitude double,
                        longitude double,
                        altitude_feet int,
                        waypoint_type string,
                        country_code string,
                        region string,
                        frequency_mhz double,
                        magnetic_variation double
                    )
                    LOCATION 's3://aeronav-iceberg-data-dev-073118366505/navigation_waypoints/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aeronav_db.navigation_waypoints VALUES
                    ('WP001', 'KENNEDY', 40.6413, -73.7781, 13, 'VOR', 'US', 'NY', 115.9, -13.2),
                    ('WP002', 'LAGUARDIA', 40.7769, -73.8740, 21, 'ILS', 'US', 'NY', 110.3, -13.1),
                    ('WP003', 'NEWARK', 40.6895, -74.1745, 9, 'VOR', 'US', 'NJ', 108.4, -12.8),
                    ('WP004', 'TETERBORO', 40.8501, -74.0606, 9, 'NDB', 'US', 'NJ', 0.0, -12.9),
                    ('WP005', 'WESTCHESTER', 41.0669, -73.7076, 439, 'GPS', 'US', 'NY', 0.0, -13.3),
                    ('WP006', 'BRIDGEPORT', 41.1637, -73.1261, 9, 'VOR', 'US', 'CT', 108.8, -14.1),
                    ('WP007', 'ISLIP', 40.7952, -73.1002, 99, 'ILS', 'US', 'NY', 109.5, -13.8),
                    ('WP008', 'MONTAUK', 41.0761, -71.9581, 6, 'NDB', 'US', 'NY', 0.0, -14.5),
                    ('WP009', 'BLOCK_ISLAND', 41.1681, -71.5804, 108, 'GPS', 'US', 'RI', 0.0, -14.8),
                    ('WP010', 'PROVIDENCE', 41.7240, -71.4128, 55, 'VOR', 'US', 'RI', 116.1, -15.2)
                    """
                },
                {
                    'name': 'flight_routes',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aeronav_db.flight_routes (
                        route_id string,
                        route_name string,
                        origin_airport string,
                        destination_airport string,
                        distance_nm int,
                        estimated_time_minutes int,
                        route_type string,
                        altitude_profile string,
                        fuel_consumption_gallons int,
                        weather_dependency string
                    )
                    LOCATION 's3://aeronav-iceberg-data-dev-073118366505/flight_routes/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aeronav_db.flight_routes VALUES
                    ('RT001', 'NYC-BOS-EXPRESS', 'JFK', 'BOS', 187, 45, 'DIRECT', 'FL350', 850, 'LOW'),
                    ('RT002', 'NYC-DC-SHUTTLE', 'LGA', 'DCA', 214, 55, 'AIRWAY', 'FL280', 920, 'MEDIUM'),
                    ('RT003', 'NYC-MIA-COASTAL', 'JFK', 'MIA', 1089, 165, 'OCEANIC', 'FL380', 4200, 'HIGH'),
                    ('RT004', 'NYC-CHI-MIDWEST', 'EWR', 'ORD', 719, 120, 'AIRWAY', 'FL360', 2800, 'MEDIUM'),
                    ('RT005', 'NYC-LAX-TRANSCONTINENTAL', 'JFK', 'LAX', 2475, 315, 'JETSTREAM', 'FL400', 9500, 'HIGH'),
                    ('RT006', 'NYC-ATL-SOUTHEAST', 'LGA', 'ATL', 762, 125, 'DIRECT', 'FL340', 2950, 'LOW'),
                    ('RT007', 'NYC-DEN-MOUNTAIN', 'EWR', 'DEN', 1626, 240, 'AIRWAY', 'FL390', 6200, 'HIGH'),
                    ('RT008', 'NYC-SEA-NORTHERN', 'JFK', 'SEA', 2408, 305, 'POLAR', 'FL410', 9200, 'HIGH'),
                    ('RT009', 'NYC-PHX-DESERT', 'LGA', 'PHX', 2145, 285, 'DIRECT', 'FL370', 8100, 'MEDIUM'),
                    ('RT010', 'NYC-SFO-PACIFIC', 'EWR', 'SFO', 2565, 325, 'OCEANIC', 'FL420', 9800, 'HIGH')
                    """
                }
            ]
        },
        {
            'name': 'AeroWeather',
            'database': 'aeroweather_db',
            'bucket': 'aeroweather-iceberg-data-dev-073118366505',
            'tables': [
                {
                    'name': 'weather_observations',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aeroweather_db.weather_observations (
                        observation_id string,
                        airport_code string,
                        observation_time timestamp,
                        temperature_celsius double,
                        humidity_percent int,
                        wind_speed_knots int,
                        wind_direction_degrees int,
                        visibility_miles double,
                        cloud_coverage string,
                        barometric_pressure_hpa double
                    )
                    LOCATION 's3://aeroweather-iceberg-data-dev-073118366505/weather_observations/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aeroweather_db.weather_observations VALUES
                    ('OBS001', 'JFK', timestamp '2024-01-15 14:00:00', 8.5, 65, 15, 270, 10.0, 'SCT', 1013.2),
                    ('OBS002', 'LGA', timestamp '2024-01-15 14:00:00', 9.2, 62, 18, 280, 8.5, 'BKN', 1012.8),
                    ('OBS003', 'EWR', timestamp '2024-01-15 14:00:00', 7.8, 68, 12, 260, 9.2, 'FEW', 1014.1),
                    ('OBS004', 'BOS', timestamp '2024-01-15 14:00:00', 5.1, 72, 22, 290, 6.8, 'OVC', 1011.5),
                    ('OBS005', 'DCA', timestamp '2024-01-15 14:00:00', 12.3, 58, 8, 180, 12.0, 'CLR', 1015.3),
                    ('OBS006', 'ATL', timestamp '2024-01-15 14:00:00', 18.7, 45, 5, 120, 15.0, 'SCT', 1016.8),
                    ('OBS007', 'ORD', timestamp '2024-01-15 14:00:00', 2.1, 78, 25, 320, 4.2, 'BKN', 1009.7),
                    ('OBS008', 'LAX', timestamp '2024-01-15 14:00:00', 22.4, 35, 3, 240, 18.0, 'CLR', 1018.9),
                    ('OBS009', 'MIA', timestamp '2024-01-15 14:00:00', 26.8, 82, 12, 110, 8.0, 'SCT', 1019.2),
                    ('OBS010', 'SEA', timestamp '2024-01-15 14:00:00', 11.2, 88, 18, 200, 3.5, 'OVC', 1008.4)
                    """
                },
                {
                    'name': 'weather_forecasts',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aeroweather_db.weather_forecasts (
                        forecast_id string,
                        airport_code string,
                        forecast_time timestamp,
                        valid_time timestamp,
                        predicted_temp_celsius double,
                        predicted_wind_speed_knots int,
                        predicted_wind_direction_degrees int,
                        precipitation_probability_percent int,
                        forecast_confidence string,
                        severe_weather_risk string
                    )
                    LOCATION 's3://aeroweather-iceberg-data-dev-073118366505/weather_forecasts/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aeroweather_db.weather_forecasts VALUES
                    ('FC001', 'JFK', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 6.2, 20, 280, 15, 'HIGH', 'LOW'),
                    ('FC002', 'LGA', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 7.1, 22, 290, 18, 'HIGH', 'LOW'),
                    ('FC003', 'EWR', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 5.8, 18, 270, 12, 'MEDIUM', 'LOW'),
                    ('FC004', 'BOS', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 3.2, 28, 310, 35, 'MEDIUM', 'MODERATE'),
                    ('FC005', 'DCA', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 10.5, 12, 200, 8, 'HIGH', 'LOW'),
                    ('FC006', 'ATL', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 16.3, 8, 140, 25, 'HIGH', 'LOW'),
                    ('FC007', 'ORD', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', -1.2, 32, 330, 45, 'LOW', 'HIGH'),
                    ('FC008', 'LAX', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 20.8, 5, 250, 2, 'HIGH', 'LOW'),
                    ('FC009', 'MIA', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 24.5, 15, 120, 60, 'MEDIUM', 'MODERATE'),
                    ('FC010', 'SEA', timestamp '2024-01-15 12:00:00', timestamp '2024-01-15 18:00:00', 9.8, 22, 210, 75, 'LOW', 'MODERATE')
                    """
                }
            ]
        },
        {
            'name': 'AeroTraffic',
            'database': 'aerotraffic_db',
            'bucket': 'aerotraffic-iceberg-data-dev-073118366505',
            'tables': [
                {
                    'name': 'air_traffic_control',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aerotraffic_db.air_traffic_control (
                        control_id string,
                        sector_name string,
                        controller_callsign string,
                        frequency_mhz double,
                        active_flights int,
                        traffic_density string,
                        weather_impact string,
                        delay_minutes int,
                        coordination_required boolean,
                        emergency_status string
                    )
                    LOCATION 's3://aerotraffic-iceberg-data-dev-073118366505/air_traffic_control/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aerotraffic_db.air_traffic_control VALUES
                    ('ATC001', 'NY_APPROACH', 'N90_APP', 119.2, 45, 'HIGH', 'MODERATE', 12, true, 'NORMAL'),
                    ('ATC002', 'NY_DEPARTURE', 'N90_DEP', 120.8, 38, 'HIGH', 'LOW', 8, false, 'NORMAL'),
                    ('ATC003', 'NY_CENTER', 'ZNY_CTR', 134.7, 67, 'VERY_HIGH', 'HIGH', 25, true, 'ALERT'),
                    ('ATC004', 'BOS_APPROACH', 'A90_APP', 118.3, 32, 'MEDIUM', 'MODERATE', 15, true, 'NORMAL'),
                    ('ATC005', 'DC_APPROACH', 'PCT_APP', 119.1, 41, 'HIGH', 'LOW', 6, false, 'NORMAL'),
                    ('ATC006', 'ATL_APPROACH', 'A80_APP', 120.9, 58, 'VERY_HIGH', 'LOW', 3, false, 'NORMAL'),
                    ('ATC007', 'CHI_CENTER', 'ZAU_CTR', 135.2, 72, 'VERY_HIGH', 'HIGH', 35, true, 'ALERT'),
                    ('ATC008', 'LAX_APPROACH', 'SCT_APP', 124.9, 49, 'HIGH', 'LOW', 5, false, 'NORMAL'),
                    ('ATC009', 'MIA_APPROACH', 'MIA_APP', 118.5, 36, 'MEDIUM', 'MODERATE', 18, true, 'NORMAL'),
                    ('ATC010', 'SEA_CENTER', 'ZSE_CTR', 133.4, 28, 'MEDIUM', 'HIGH', 22, true, 'CAUTION')
                    """
                },
                {
                    'name': 'runway_operations',
                    'create_sql': """
                    CREATE TABLE IF NOT EXISTS aerotraffic_db.runway_operations (
                        operation_id string,
                        airport_code string,
                        runway_id string,
                        operation_type string,
                        aircraft_type string,
                        operation_time timestamp,
                        flight_number string,
                        gate_assignment string,
                        taxi_time_minutes int,
                        fuel_consumed_gallons int
                    )
                    LOCATION 's3://aerotraffic-iceberg-data-dev-073118366505/runway_operations/'
                    TBLPROPERTIES (
                        'table_type'='ICEBERG',
                        'format'='parquet'
                    )
                    """,
                    'insert_sql': """
                    INSERT INTO aerotraffic_db.runway_operations VALUES
                    ('ROP001', 'JFK', '04L', 'DEPARTURE', 'Boeing 737', timestamp '2024-01-15 15:30:00', 'UA1234', 'T4-12', 18, 1200),
                    ('ROP002', 'JFK', '04R', 'ARRIVAL', 'Airbus A320', timestamp '2024-01-15 15:32:00', 'DL5678', 'T2-8', 12, 0),
                    ('ROP003', 'LGA', '13', 'DEPARTURE', 'Embraer E190', timestamp '2024-01-15 15:35:00', 'AA9012', 'B-15', 15, 850),
                    ('ROP004', 'EWR', '04L', 'ARRIVAL', 'Boeing 777', timestamp '2024-01-15 15:38:00', 'UA3456', 'C-92', 20, 0),
                    ('ROP005', 'BOS', '04R', 'DEPARTURE', 'Boeing 787', timestamp '2024-01-15 15:40:00', 'JB7890', 'A-7', 22, 2800),
                    ('ROP006', 'DCA', '01', 'ARRIVAL', 'Airbus A319', timestamp '2024-01-15 15:42:00', 'WN1122', 'A-12', 8, 0),
                    ('ROP007', 'ATL', '08R', 'DEPARTURE', 'Boeing 757', timestamp '2024-01-15 15:45:00', 'DL3344', 'T-45', 25, 1950),
                    ('ROP008', 'ORD', '10L', 'ARRIVAL', 'Airbus A330', timestamp '2024-01-15 15:47:00', 'UA5566', 'F-18', 28, 0),
                    ('ROP009', 'LAX', '24R', 'DEPARTURE', 'Boeing 747', timestamp '2024-01-15 15:50:00', 'AA7788', 'TBIT-156', 35, 4200),
                    ('ROP010', 'MIA', '08R', 'ARRIVAL', 'Airbus A350', timestamp '2024-01-15 15:52:00', 'AA9900', 'D-42', 18, 0)
                    """
                }
            ]
        }
    ]
    
    print("🚀 Setting up DataLounge Iceberg tables from WingSafe centralized catalog...")
    print("=" * 80)
    
    for app in applications:
        print(f"\n🔧 Setting up {app['name']} Application...")
        
        for table in app['tables']:
            print(f"\n  📊 Setting up table: {table['name']}")
            
            # Create Iceberg table
            if not execute_athena_query(table['create_sql'], f"Creating {table['name']} table"):
                continue
            
            # Insert sample data
            if not execute_athena_query(table['insert_sql'], f"Inserting sample data into {table['name']}"):
                continue
            
            print(f"  ✅ {table['name']} table setup complete")
        
        print(f"✅ {app['name']} Application setup complete")
    
    print("\n🎉 DataLounge table creation and data insertion complete!")
    print("\n📋 Centralized setup:")
    print("• Catalog: WingSafe account (184838390535)")
    print("• Data storage: DataLounge S3 buckets (073118366505)")
    print("• Column permissions: WingSafe LakeFormation")
    print("• User access: AeroInsight account via cross-account roles")
    
    return True

if __name__ == "__main__":
    setup_datalounge_tables_and_data()