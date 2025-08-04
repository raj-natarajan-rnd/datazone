import boto3
import time
from datetime import datetime
from tabulate import tabulate

def assume_role_and_get_athena_client(role_arn):
    """Assume cross-account role and return Athena client"""
    sts = boto3.client('sts')
    
    try:
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='DataLoungeDemo'
        )
        
        credentials = response['Credentials']
        
        return boto3.client(
            'athena',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    except Exception as e:
        print(f"Failed to assume role {role_arn}: {e}")
        return None

def execute_athena_query(athena_client, query, description):
    """Execute an Athena query and return results"""
    try:
        print(f"{description}...")
        
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': 's3://wingsafe-athena-results-dev-184838390535/demo-results/'
            },
            WorkGroup='WingSafe-DataAnalysis-dev'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for completion
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            time.sleep(2)
        
        if status == 'SUCCEEDED':
            result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            return {
                'success': True,
                'columns': [col['Name'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']],
                'data': result['ResultSet']['Rows'][1:],  # Skip header row
                'row_count': len(result['ResultSet']['Rows']) - 1
            }
        else:
            error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            return {
                'success': False,
                'error': error_msg,
                'status': status
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'status': 'EXCEPTION'
        }

def demo_datalounge_multi_application():
    """Comprehensive demo of DataLounge multi-application setup with column-level security"""
    
    print("DATALOUNGE MULTI-APPLICATION DEMO")
    print("=" * 80)
    print(f"Demo Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Account: AeroInsight (707843606641)")
    print(f"Security: LakeFormation Column-Level Permissions")
    print("=" * 80)
    
    # Demo scenarios with role ARNs
    demo_scenarios = [
        {
            'title': 'Full Data Access (DataScientist)',
            'role': 'DataScientist',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev',
            'queries': [
                {
                    'name': 'AeroNav Navigation Data',
                    'sql': 'SELECT * FROM aeronav_db.navigation_waypoints LIMIT 2;',
                    'description': 'Should show all columns including restricted frequency_mhz, magnetic_variation'
                },
                {
                    'name': 'AeroWeather Detailed Data',
                    'sql': 'SELECT * FROM aeroweather_db.weather_observations LIMIT 2;',
                    'description': 'Should show all columns including restricted wind and pressure data'
                },
                {
                    'name': 'AeroTraffic Sensitive Data',
                    'sql': 'SELECT * FROM aerotraffic_db.air_traffic_control LIMIT 2;',
                    'description': 'Should show all columns including restricted frequency_mhz, emergency_status'
                }
            ]
        },
        {
            'title': 'Restricted AeroNav Access',
            'role': 'AeroNav',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroNav-CrossAccount-dev',
            'queries': [
                {
                    'name': 'Navigation Waypoints (Restricted)',
                    'sql': 'SELECT * FROM aeronav_db.navigation_waypoints LIMIT 2;',
                    'description': 'Should NOT show frequency_mhz and magnetic_variation columns'
                },
                {
                    'name': 'Flight Routes (Restricted)',
                    'sql': 'SELECT * FROM aeronav_db.flight_routes LIMIT 2;',
                    'description': 'Should NOT show fuel_consumption_gallons and altitude_profile columns'
                }
            ]
        },
        {
            'title': 'Restricted AeroWeather Access',
            'role': 'AeroWeather',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroWeather-CrossAccount-dev',
            'queries': [
                {
                    'name': 'Weather Observations (Restricted)',
                    'sql': 'SELECT * FROM aeroweather_db.weather_observations LIMIT 2;',
                    'description': 'Should NOT show barometric_pressure_hpa, wind_speed_knots, wind_direction_degrees'
                },
                {
                    'name': 'Weather Forecasts (Restricted)',
                    'sql': 'SELECT * FROM aeroweather_db.weather_forecasts LIMIT 2;',
                    'description': 'Should NOT show predicted wind data and forecast_confidence'
                }
            ]
        },
        {
            'title': 'Restricted AeroTraffic Access',
            'role': 'AeroTraffic',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-AeroTraffic-CrossAccount-dev',
            'queries': [
                {
                    'name': 'Air Traffic Control (Restricted)',
                    'sql': 'SELECT * FROM aerotraffic_db.air_traffic_control LIMIT 2;',
                    'description': 'Should NOT show frequency_mhz, coordination_required, emergency_status'
                },
                {
                    'name': 'Runway Operations (Restricted)',
                    'sql': 'SELECT * FROM aerotraffic_db.runway_operations LIMIT 2;',
                    'description': 'Should NOT show fuel_consumed_gallons, taxi_time_minutes'
                }
            ]
        }
    ]
    
    for scenario in demo_scenarios:
        print(f"\n{scenario['title']}")
        print(f"Role: {scenario['role']}")
        print(f"Role ARN: {scenario['role_arn']}")
        print("=" * 80)
        
        # Assume role and get Athena client
        athena_client = assume_role_and_get_athena_client(scenario['role_arn'])
        if not athena_client:
            print(f"Failed to assume role for {scenario['role']}")
            continue
        
        for i, query in enumerate(scenario['queries'], 1):
            print(f"\nTEST {i}: {query['name']}")
            print(f"Description: {query['description']}")
            print(f"SQL: {query['sql']}")
            print("-" * 60)
            
            result = execute_athena_query(athena_client, query['sql'], f"Executing {query['name']}")
            
            if result['success']:
                print("QUERY SUCCESSFUL")
                print(f"Columns ({len(result['columns'])}): {', '.join(result['columns'])}")
                
                # Check for restricted columns
                restricted_columns = {
                    'AeroNav': ['frequency_mhz', 'magnetic_variation', 'fuel_consumption_gallons', 'altitude_profile'],
                    'AeroWeather': ['barometric_pressure_hpa', 'wind_speed_knots', 'wind_direction_degrees', 'predicted_wind_speed_knots', 'predicted_wind_direction_degrees', 'forecast_confidence'],
                    'AeroTraffic': ['frequency_mhz', 'coordination_required', 'emergency_status', 'fuel_consumed_gallons', 'taxi_time_minutes']
                }
                
                if scenario['role'] == 'DataScientist':
                    print("EXPECTED: Full access to all columns (DataScientist privilege)")
                else:
                    role_restricted = restricted_columns.get(scenario['role'], [])
                    has_restricted = any(col in role_restricted for col in result['columns'])
                    
                    if has_restricted:
                        print("UNEXPECTED: Restricted columns should be hidden for this role")
                    else:
                        print("CORRECT: Restricted columns properly hidden")
                
                # Display sample data
                if result['row_count'] > 0:
                    print(f"Rows returned: {result['row_count']}")
                    print("\nSAMPLE DATA:")
                    
                    # Convert data to table format
                    table_data = []
                    for row in result['data'][:2]:  # Show max 2 rows
                        table_row = []
                        for field in row['Data']:
                            if 'VarCharValue' in field:
                                table_row.append(field['VarCharValue'])
                            else:
                                table_row.append('NULL')
                        table_data.append(table_row)
                    
                    if table_data:
                        print(tabulate(table_data, headers=result['columns'], tablefmt='grid', maxcolwidths=12))
            
            else:
                print("QUERY FAILED")
                print(f"Error: {result['error']}")
                
                # Check if failure is expected due to column restrictions
                if scenario['role'] != 'DataScientist':
                    restricted_keywords = ['frequency_mhz', 'magnetic_variation', 'barometric_pressure_hpa', 
                                         'coordination_required', 'emergency_status', 'fuel_consumed_gallons']
                    if any(keyword in result['error'].lower() for keyword in restricted_keywords):
                        print("EXPECTED: Column access denied - Security working correctly!")
            
            print("-" * 60)
        
        print("=" * 80)
    
    print(f"\nDATALOUNGE MULTI-APPLICATION DEMO COMPLETE!")
    print("\nKEY SECURITY FEATURES DEMONSTRATED:")
    print("• Multi-application role-based column access control")
    print("• LakeFormation integration with Athena")
    print("• Cross-account security enforcement")
    print("• Application-specific data isolation")
    print("• Centralized governance with distributed storage")

if __name__ == "__main__":
    demo_datalounge_multi_application()