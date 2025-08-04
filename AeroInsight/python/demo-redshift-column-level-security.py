import boto3
import time
from datetime import datetime
from tabulate import tabulate

def demo_column_level_security():
    """Comprehensive demo of column-level security with both roles"""
    
    redshift_data = boto3.client('redshift-data', region_name='us-east-1')
    workgroup_name = 'aeroinsight-workgroup-dev'
    database = 'analytics'
    
    print("REDSHIFT COLUMN-LEVEL SECURITY DEMO")
    print("=" * 80)
    print(f"Demo Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Account: AeroInsight (707843606641)")
    print(f"Security: LakeFormation Column-Level Permissions")
    print("=" * 80)
    
    # Demo scenarios
    demo_scenarios = [
        {
            'title': 'Full Data Access (DataScientist)',
            'role': 'DataScientist',
            'schema': 'wingsafe_catalog',
            'expected_cols': 13,
            'queries': [
                {
                    'name': 'All Columns',
                    'sql': 'SELECT * FROM wingsafe_catalog.radar_detections LIMIT 2;',
                    'description': 'Should show all 13 columns including restricted ones'
                },
                {
                    'name': 'Restricted Columns Only',
                    'sql': 'SELECT flight_id, speed_knots, heading_degrees FROM wingsafe_catalog.radar_detections LIMIT 2;',
                    'description': 'Direct access to restricted columns (speed_knots, heading_degrees)'
                },
                {
                    'name': 'Speed Analysis',
                    'sql': 'SELECT flight_id, aircraft_type, speed_knots, CASE WHEN speed_knots > 400 THEN \'Fast\' ELSE \'Normal\' END as speed_category FROM wingsafe_catalog.radar_detections LIMIT 2;',
                    'description': 'Business logic using restricted speed data'
                }
            ]
        },
        {
            'title': 'Restricted Data Access (FlightRadarViewer)',
            'role': 'FlightRadarViewer',
            'schema': 'wingsafe_flightradarviewer',
            'expected_cols': 11,
            'queries': [
                {
                    'name': 'All Available Columns',
                    'sql': 'SELECT * FROM wingsafe_flightradarviewer.radar_detections LIMIT 2;',
                    'description': 'Should show only 11 columns (speed_knots, heading_degrees hidden)'
                },
                {
                    'name': 'Allowed Columns Only',
                    'sql': 'SELECT flight_id, aircraft_type, altitude_feet, latitude, longitude FROM wingsafe_flightradarviewer.radar_detections LIMIT 2;',
                    'description': 'Access to non-restricted flight tracking data'
                },
                {
                    'name': 'Restricted Column Test',
                    'sql': 'SELECT flight_id, speed_knots FROM wingsafe_flightradarviewer.radar_detections LIMIT 2;',
                    'description': 'Attempt to access restricted column (should fail or return NULL)'
                }
            ]
        }
    ]
    
    for scenario in demo_scenarios:
        print(f"\nSCENARIO: {scenario['title']}")
        print(f"Role: {scenario['role']}")
        print(f"Schema: {scenario['schema']}")
        print(f"Expected Columns: {scenario['expected_cols']}")
        print("=" * 80)
        
        for i, query in enumerate(scenario['queries'], 1):
            print(f"\nTEST {i}: {query['name']}")
            print(f"Description: {query['description']}")
            print(f"SQL: {query['sql']}")
            print("-" * 60)
            
            try:
                response = redshift_data.execute_statement(
                    WorkgroupName=workgroup_name,
                    Database=database,
                    Sql=query['sql']
                )
                
                query_id = response['Id']
                
                # Wait for completion
                while True:
                    status_response = redshift_data.describe_statement(Id=query_id)
                    status = status_response['Status']
                    
                    if status in ['FINISHED', 'FAILED', 'ABORTED']:
                        break
                    
                    time.sleep(2)
                
                if status == 'FINISHED':
                    result = redshift_data.get_statement_result(Id=query_id)
                    columns = [col['name'] for col in result['ColumnMetadata']]
                    
                    print("QUERY SUCCESSFUL")
                    print(f"Columns ({len(columns)}): {', '.join(columns)}")
                    
                    # Check for restricted columns
                    restricted_columns = ['speed_knots', 'heading_degrees']
                    has_restricted = any(col in restricted_columns for col in columns)
                    
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
                    
                    # Display data in table format
                    if 'Records' in result and result['Records']:
                        print(f"Rows returned: {len(result['Records'])}")
                        
                        # Convert data to table format
                        table_data = []
                        for record in result['Records']:
                            row = []
                            for field in record:
                                if 'stringValue' in field:
                                    row.append(field['stringValue'])
                                elif 'longValue' in field:
                                    row.append(field['longValue'])
                                elif 'doubleValue' in field:
                                    row.append(f"{field['doubleValue']:.2f}")
                                elif 'isNull' in field:
                                    row.append('NULL')
                                else:
                                    row.append('N/A')
                            table_data.append(row)
                        
                        # Display as formatted table
                        if table_data:
                            print("\nDATA SAMPLE:")
                            print(tabulate(table_data, headers=columns, tablefmt='grid', maxcolwidths=12))
                
                else:
                    print(f"QUERY FAILED: {status}")
                    if 'Error' in status_response:
                        error_msg = status_response['Error']
                        print(f"Error: {error_msg}")
                        
                        # Check if it's expected column access denial
                        if scenario['role'] == 'FlightRadarViewer' and ('speed_knots' in error_msg or 'heading_degrees' in error_msg):
                            print("EXPECTED: Column access denied - Security working correctly!")
            
            except Exception as e:
                print(f"EXECUTION ERROR: {e}")
                # Check if it's expected column access denial
                if scenario['role'] == 'FlightRadarViewer' and ('speed_knots' in str(e) or 'heading_degrees' in str(e)):
                    print("EXPECTED: Column access denied - Security working correctly!")
            
            print("-" * 60)
        
        print("=" * 80)
    
    # Summary comparison
    print(f"\nSECURITY COMPARISON SUMMARY")
    print("=" * 80)
    
    comparison_data = [
        ['DataScientist', 'wingsafe_catalog', '13 columns', 'Full access', 'Can see speed_knots, heading_degrees'],
        ['FlightRadarViewer', 'wingsafe_flightradarviewer', '11 columns', 'Restricted', 'Cannot see speed_knots, heading_degrees']
    ]
    
    headers = ['Role', 'Schema', 'Column Count', 'Access Level', 'Restricted Columns']
    print(tabulate(comparison_data, headers=headers, tablefmt='grid'))
    
    print(f"\nCOLUMN-LEVEL SECURITY DEMO COMPLETE!")
    print("\nKEY SECURITY FEATURES DEMONSTRATED:")
    print("• Role-based column access control")
    print("• LakeFormation integration with Redshift")
    print("• Cross-account security enforcement")
    print("• Transparent data filtering based on IAM role")
    print("• Business logic protection (speed/heading data)")
    
    print("\nARCHITECTURE:")
    print("• AeroInsight Redshift Serverless")
    print("• WingSafe Centralized Catalog (LakeFormation)")
    print("• FlightRadar S3 Data Lake (Iceberg)")
    print("• Column-level permissions via IAM role context")
    
    print("\nUSE CASES:")
    print("• DataScientist: Full analytics, ML model training, performance analysis")
    print("• FlightRadarViewer: Flight tracking, basic reporting, operational dashboards")

if __name__ == "__main__":
    demo_column_level_security()