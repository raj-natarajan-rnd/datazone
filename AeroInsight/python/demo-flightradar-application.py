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
            RoleSessionName='FlightRadarDemo'
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

def demo_flightradar_application():
    """Comprehensive demo of FlightRadar application with column-level security"""
    
    print("FLIGHTRADAR APPLICATION DEMO")
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
                    'name': 'All Radar Data',
                    'sql': 'SELECT * FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Should show all 13 columns including restricted speed_knots, heading_degrees'
                },
                {
                    'name': 'Restricted Columns Only',
                    'sql': 'SELECT flight_id, speed_knots, heading_degrees FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Direct access to restricted columns (speed_knots, heading_degrees)'
                },
                {
                    'name': 'Speed Analysis',
                    'sql': 'SELECT flight_id, aircraft_type, speed_knots, CASE WHEN speed_knots > 400 THEN \'Fast\' ELSE \'Normal\' END as speed_category FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Business logic using restricted speed data'
                }
            ]
        },
        {
            'title': 'Restricted FlightRadar Access',
            'role': 'FlightRadarViewer',
            'role_arn': 'arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev',
            'queries': [
                {
                    'name': 'All Available Columns',
                    'sql': 'SELECT * FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Should show only 11 columns (speed_knots, heading_degrees hidden)'
                },
                {
                    'name': 'Allowed Columns Only',
                    'sql': 'SELECT flight_id, aircraft_type, altitude_feet, latitude, longitude FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Access to non-restricted flight tracking data'
                },
                {
                    'name': 'Restricted Column Test',
                    'sql': 'SELECT flight_id, speed_knots FROM flightradar_db.radar_detections LIMIT 2;',
                    'description': 'Attempt to access restricted column (should fail or return NULL)'
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
                restricted_columns = ['speed_knots', 'heading_degrees']
                has_restricted = any(col in restricted_columns for col in result['columns'])
                
                if scenario['role'] == 'DataScientist':
                    if has_restricted:
                        print("EXPECTED: Restricted columns visible (DataScientist privilege)")
                    else:
                        print("No restricted columns in this query")
                else:  # FlightRadarViewer
                    if has_restricted:
                        print("UNEXPECTED: Restricted columns should be hidden for FlightRadarViewer")
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
                if scenario['role'] == 'FlightRadarViewer':
                    restricted_keywords = ['speed_knots', 'heading_degrees']
                    if any(keyword in result['error'].lower() for keyword in restricted_keywords):
                        print("EXPECTED: Column access denied - Security working correctly!")
            
            print("-" * 60)
        
        print("=" * 80)
    
    # Summary comparison
    print(f"\nSECURITY COMPARISON SUMMARY")
    print("=" * 80)
    
    comparison_data = [
        ['DataScientist', 'flightradar_db', '13 columns', 'Full access', 'Can see speed_knots, heading_degrees'],
        ['FlightRadarViewer', 'flightradar_db', '11 columns', 'Restricted', 'Cannot see speed_knots, heading_degrees']
    ]
    
    headers = ['Role', 'Database', 'Column Access', 'Access Level', 'Restrictions']
    print(tabulate(comparison_data, headers=headers, tablefmt='grid'))
    
    print(f"\nFLIGHTRADAR APPLICATION DEMO COMPLETE!")
    print("\nKEY SECURITY FEATURES DEMONSTRATED:")
    print("• Role-based column access control")
    print("• LakeFormation integration with Athena")
    print("• Cross-account security enforcement")
    print("• Transparent data filtering based on IAM role")
    print("• Business logic protection (speed/heading data)")
    
    print("\nARCHITECTURE:")
    print("• AeroInsight Athena Queries")
    print("• WingSafe Centralized Catalog (LakeFormation)")
    print("• FlightRadar S3 Data Lake (Iceberg)")
    print("• Column-level permissions via IAM role context")

if __name__ == "__main__":
    demo_flightradar_application()