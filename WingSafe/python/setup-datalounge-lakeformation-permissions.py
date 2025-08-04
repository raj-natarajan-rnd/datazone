import boto3

def setup_datalounge_lakeformation_permissions():
    """Setup LakeFormation column-level permissions for DataLounge applications"""
    lakeformation = boto3.client('lakeformation')
    
    # Role ARNs
    datascientist_role = "arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev"
    
    # Application-specific roles
    aeronav_role = "arn:aws:iam::184838390535:role/WingSafe-AeroNav-CrossAccount-dev"
    aeroweather_role = "arn:aws:iam::184838390535:role/WingSafe-AeroWeather-CrossAccount-dev"
    aerotraffic_role = "arn:aws:iam::184838390535:role/WingSafe-AeroTraffic-CrossAccount-dev"
    
    # Application configurations with restricted columns
    applications = [
        {
            'name': 'AeroNav',
            'database': 'aeronav_db',
            'role': aeronav_role,
            'tables': [
                {
                    'name': 'navigation_waypoints',
                    'restricted_columns': ['frequency_mhz', 'magnetic_variation'],  # Hide sensitive navigation data
                    'all_columns': ['waypoint_id', 'waypoint_name', 'latitude', 'longitude', 'altitude_feet', 
                                  'waypoint_type', 'country_code', 'region', 'frequency_mhz', 'magnetic_variation']
                },
                {
                    'name': 'flight_routes',
                    'restricted_columns': ['fuel_consumption_gallons', 'altitude_profile'],  # Hide operational details
                    'all_columns': ['route_id', 'route_name', 'origin_airport', 'destination_airport', 'distance_nm',
                                  'estimated_time_minutes', 'route_type', 'altitude_profile', 'fuel_consumption_gallons', 'weather_dependency']
                }
            ]
        },
        {
            'name': 'AeroWeather',
            'database': 'aeroweather_db',
            'role': aeroweather_role,
            'tables': [
                {
                    'name': 'weather_observations',
                    'restricted_columns': ['barometric_pressure_hpa', 'wind_speed_knots', 'wind_direction_degrees'],  # Hide detailed weather data
                    'all_columns': ['observation_id', 'airport_code', 'observation_time', 'temperature_celsius', 'humidity_percent',
                                  'wind_speed_knots', 'wind_direction_degrees', 'visibility_miles', 'cloud_coverage', 'barometric_pressure_hpa']
                },
                {
                    'name': 'weather_forecasts',
                    'restricted_columns': ['predicted_wind_speed_knots', 'predicted_wind_direction_degrees', 'forecast_confidence'],  # Hide forecast details
                    'all_columns': ['forecast_id', 'airport_code', 'forecast_time', 'valid_time', 'predicted_temp_celsius',
                                  'predicted_wind_speed_knots', 'predicted_wind_direction_degrees', 'precipitation_probability_percent', 
                                  'forecast_confidence', 'severe_weather_risk']
                }
            ]
        },
        {
            'name': 'AeroTraffic',
            'database': 'aerotraffic_db',
            'role': aerotraffic_role,
            'tables': [
                {
                    'name': 'air_traffic_control',
                    'restricted_columns': ['frequency_mhz', 'coordination_required', 'emergency_status'],  # Hide sensitive ATC data
                    'all_columns': ['control_id', 'sector_name', 'controller_callsign', 'frequency_mhz', 'active_flights',
                                  'traffic_density', 'weather_impact', 'delay_minutes', 'coordination_required', 'emergency_status']
                },
                {
                    'name': 'runway_operations',
                    'restricted_columns': ['fuel_consumed_gallons', 'taxi_time_minutes'],  # Hide operational metrics
                    'all_columns': ['operation_id', 'airport_code', 'runway_id', 'operation_type', 'aircraft_type',
                                  'operation_time', 'flight_number', 'gate_assignment', 'taxi_time_minutes', 'fuel_consumed_gallons']
                }
            ]
        }
    ]
    
    try:
        print("🔧 Setting up DataLounge LakeFormation column-level permissions...")
        print("=" * 80)
        
        # Step 1: Revoke IAM_ALLOWED_PRINCIPALS if exists for each database
        print("1. Checking IAM_ALLOWED_PRINCIPALS permissions...")
        for app in applications:
            for table in app['tables']:
                try:
                    permissions = lakeformation.list_permissions(
                        Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}
                    )
                    
                    for perm in permissions['PrincipalResourcePermissions']:
                        if (perm.get('Resource', {}).get('Table', {}).get('DatabaseName') == app['database'] and
                            perm.get('Resource', {}).get('Table', {}).get('Name') == table['name']):
                            print(f"Found IAM_ALLOWED_PRINCIPALS for {app['database']}.{table['name']} - revoking...")
                            lakeformation.revoke_permissions(
                                Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'},
                                Resource={
                                    'Table': {
                                        'CatalogId': '184838390535',
                                        'DatabaseName': app['database'],
                                        'Name': table['name']
                                    }
                                },
                                Permissions=['ALL']
                            )
                            print(f"✅ IAM_ALLOWED_PRINCIPALS revoked for {table['name']}")
                except Exception as e:
                    print(f"⚠️ Manual revoke may be needed for {app['database']}.{table['name']}: {e}")
        
        # Step 2: Setup permissions for each application
        for app in applications:
            print(f"\n🔧 Setting up {app['name']} permissions...")
            
            # Grant database permissions
            print(f"  📊 Granting database permissions for {app['database']}")
            for role_arn in [datascientist_role, app['role']]:
                try:
                    lakeformation.grant_permissions(
                        Principal={'DataLakePrincipalIdentifier': role_arn},
                        Resource={
                            'Database': {
                                'CatalogId': '184838390535',
                                'Name': app['database']
                            }
                        },
                        Permissions=['DESCRIBE']
                    )
                except Exception as e:
                    print(f"    ⚠️ Database permission may already exist: {e}")
            print(f"  ✅ Database permissions granted to both roles")
            
            # Setup table permissions
            for table in app['tables']:
                print(f"  📋 Setting up {table['name']} table permissions...")
                
                # DataScientist gets FULL access to all columns
                try:
                    lakeformation.grant_permissions(
                        Principal={'DataLakePrincipalIdentifier': datascientist_role},
                        Resource={
                            'Table': {
                                'CatalogId': '184838390535',
                                'DatabaseName': app['database'],
                                'Name': table['name']
                            }
                        },
                        Permissions=['SELECT']
                    )
                    print(f"    ✅ DataScientist: FULL access to {table['name']}")
                except Exception as e:
                    print(f"    ⚠️ DataScientist permission may already exist: {e}")
                
                # Application role gets RESTRICTED access (excluding sensitive columns)
                allowed_columns = [col for col in table['all_columns'] if col not in table['restricted_columns']]
                
                try:
                    lakeformation.grant_permissions(
                        Principal={'DataLakePrincipalIdentifier': app['role']},
                        Resource={
                            'TableWithColumns': {
                                'CatalogId': '184838390535',
                                'DatabaseName': app['database'],
                                'Name': table['name'],
                                'ColumnNames': allowed_columns
                            }
                        },
                        Permissions=['SELECT']
                    )
                    print(f"    ✅ {app['name']} Role: RESTRICTED access to {table['name']}")
                    print(f"       Allowed columns ({len(allowed_columns)}): {', '.join(allowed_columns)}")
                    print(f"       Hidden columns ({len(table['restricted_columns'])}): {', '.join(table['restricted_columns'])}")
                except Exception as e:
                    print(f"    ⚠️ Application role permission may already exist: {e}")
        
        print("\n🎉 DataLounge LakeFormation permissions setup complete!")
        print("\n📋 Permission Summary:")
        print("=" * 80)
        
        print("\n🔬 DataScientist Role (FULL ACCESS):")
        print("• Can access ALL columns in ALL tables across ALL applications")
        print("• Full analytics and research capabilities")
        
        print("\n🔒 Application-Specific Roles (RESTRICTED ACCESS):")
        for app in applications:
            print(f"\n• {app['name']} Role:")
            for table in app['tables']:
                allowed_count = len(table['all_columns']) - len(table['restricted_columns'])
                total_count = len(table['all_columns'])
                print(f"  - {table['name']}: {allowed_count}/{total_count} columns visible")
                print(f"    Hidden: {', '.join(table['restricted_columns'])}")
        
        print("\n🏗️ Architecture:")
        print("• Centralized catalog: WingSafe account (184838390535)")
        print("• Data storage: DataLounge S3 buckets (073118366505)")
        print("• Column permissions: WingSafe LakeFormation")
        print("• User access: AeroInsight account via cross-account roles")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    setup_datalounge_lakeformation_permissions()