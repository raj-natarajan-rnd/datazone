import boto3
import time

def verify_lakeformation_permissions():
    """Verify LakeFormation permissions are correctly set for all roles"""
    
    print("VERIFYING LAKEFORMATION PERMISSIONS")
    print("=" * 80)
    
    # Test by assuming each WingSafe role and checking Athena access
    roles_to_test = [
        {
            'name': 'DataScientist',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev',
            'databases': ['flightradar_db', 'aeronav_db', 'aeroweather_db', 'aerotraffic_db'],
            'expected_access': 'FULL'
        },
        {
            'name': 'FlightRadarViewer', 
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev',
            'databases': ['flightradar_db'],
            'expected_access': 'RESTRICTED'
        },
        {
            'name': 'AeroNav',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroNav-CrossAccount-dev',
            'databases': ['aeronav_db'],
            'expected_access': 'RESTRICTED'
        },
        {
            'name': 'AeroWeather',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroWeather-CrossAccount-dev',
            'databases': ['aeroweather_db'],
            'expected_access': 'RESTRICTED'
        },
        {
            'name': 'AeroTraffic',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroTraffic-CrossAccount-dev',
            'databases': ['aerotraffic_db'],
            'expected_access': 'RESTRICTED'
        }
    ]
    
    # Test queries for each database
    test_queries = {
        'flightradar_db': {
            'table': 'radar_detections',
            'query': 'SELECT * FROM flightradar_db.radar_detections LIMIT 1',
            'restricted_columns': ['speed_knots', 'heading_degrees']
        },
        'aeronav_db': {
            'table': 'navigation_waypoints',
            'query': 'SELECT * FROM aeronav_db.navigation_waypoints LIMIT 1',
            'restricted_columns': ['frequency_mhz', 'magnetic_variation']
        },
        'aeroweather_db': {
            'table': 'weather_observations',
            'query': 'SELECT * FROM aeroweather_db.weather_observations LIMIT 1',
            'restricted_columns': ['barometric_pressure_hpa', 'wind_speed_knots', 'wind_direction_degrees']
        },
        'aerotraffic_db': {
            'table': 'air_traffic_control',
            'query': 'SELECT * FROM aerotraffic_db.air_traffic_control LIMIT 1',
            'restricted_columns': ['frequency_mhz', 'coordination_required', 'emergency_status']
        }
    }
    
    for role_info in roles_to_test:
        print(f"\nTesting {role_info['name']} role...")
        print(f"Role ARN: {role_info['role_arn']}")
        print(f"Expected Access: {role_info['expected_access']}")
        
        try:
            # Assume the WingSafe role
            sts = boto3.client('sts', region_name='us-east-1')
            response = sts.assume_role(
                RoleArn=role_info['role_arn'],
                RoleSessionName=f'LakeFormationTest-{role_info["name"]}'
            )
            
            credentials = response['Credentials']
            
            # Create Athena client with assumed role credentials
            athena = boto3.client(
                'athena',
                region_name='us-east-1',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            # Test each database the role should have access to
            for database in role_info['databases']:
                print(f"\n  Testing {database}...")
                
                if database not in test_queries:
                    print(f"    No test query defined for {database}")
                    continue
                
                query_info = test_queries[database]
                
                response = athena.start_query_execution(
                    QueryString=query_info['query'],
                    ResultConfiguration={
                        'OutputLocation': 's3://wingsafe-athena-results-dev-184838390535/test-results/'
                    },
                    WorkGroup='WingSafe-DataAnalysis-dev'
                )
                
                query_id = response['QueryExecutionId']
                
                # Wait for completion
                while True:
                    result = athena.get_query_execution(QueryExecutionId=query_id)
                    status = result['QueryExecution']['Status']['State']
                    
                    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                        break
                    
                    time.sleep(2)
                
                if status == 'SUCCEEDED':
                    # Get results
                    results = athena.get_query_results(QueryExecutionId=query_id)
                    columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                    
                    print(f"    ✅ Query successful - {len(columns)} columns returned")
                    print(f"    Columns: {', '.join(columns)}")
                    
                    # Check for restricted columns
                    restricted_columns = query_info['restricted_columns']
                    has_restricted = any(col in restricted_columns for col in columns)
                    
                    if role_info['expected_access'] == 'FULL':
                        if has_restricted:
                            print(f"    ✅ CORRECT: {role_info['name']} has full access including restricted columns")
                        else:
                            print(f"    ⚠️  NOTE: No restricted columns found in this query")
                    else:  # RESTRICTED
                        if not has_restricted:
                            print(f"    ✅ CORRECT: {role_info['name']} has restricted access - sensitive columns hidden")
                        else:
                            print(f"    ❌ ISSUE: {role_info['name']} should NOT see restricted columns: {[col for col in columns if col in restricted_columns]}")
                else:
                    print(f"    ❌ Query failed: {status}")
                    error_reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    print(f"    Error: {error_reason}")
        
        except Exception as e:
            print(f"  ❌ Error testing {role_info['name']}: {e}")
        
        print("-" * 80)
    
    print("\nVERIFICATION SUMMARY:")
    print("✅ DataScientist should have FULL access to ALL databases and columns")
    print("✅ FlightRadarViewer should have RESTRICTED access to flightradar_db only")
    print("✅ AeroNav should have RESTRICTED access to aeronav_db only")
    print("✅ AeroWeather should have RESTRICTED access to aeroweather_db only") 
    print("✅ AeroTraffic should have RESTRICTED access to aerotraffic_db only")
    
    print("\nRESTRICTED COLUMNS BY APPLICATION:")
    print("• FlightRadar: speed_knots, heading_degrees")
    print("• AeroNav: frequency_mhz, magnetic_variation, fuel_consumption_gallons, altitude_profile")
    print("• AeroWeather: barometric_pressure_hpa, wind_speed_knots, wind_direction_degrees, forecast_confidence")
    print("• AeroTraffic: frequency_mhz, coordination_required, emergency_status, fuel_consumed_gallons, taxi_time_minutes")

if __name__ == "__main__":
    verify_lakeformation_permissions()