import boto3

def create_cross_account_catalog():
    """Create cross-account data catalog in AeroInsight to access FlightRadar data"""
    glue = boto3.client('glue')
    
    try:
        print("Creating cross-account data catalog...")
        
        # Create external database pointing to FlightRadar account
        glue.create_database(
            DatabaseInput={
                'Name': 'flightradar_external',
                'Description': 'External database pointing to FlightRadar account data',
                'Parameters': {
                    'external_account_id': '157809907894',
                    'external_database': 'flightradar_db'
                }
            }
        )
        
        print("✅ Cross-account catalog created")
        
        # Create external table reference
        glue.create_table(
            DatabaseName='flightradar_external',
            TableInput={
                'Name': 'radar_detections',
                'Description': 'External reference to FlightRadar radar detections table',
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'iceberg',
                    'table_type': 'ICEBERG',
                    'external_account_id': '157809907894'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'timestamp', 'Type': 'timestamp'},
                        {'Name': 'flight_id', 'Type': 'string'},
                        {'Name': 'aircraft_type', 'Type': 'string'},
                        {'Name': 'altitude_feet', 'Type': 'int'},
                        {'Name': 'latitude', 'Type': 'double'},
                        {'Name': 'longitude', 'Type': 'double'},
                        {'Name': 'vertical_speed', 'Type': 'int'},
                        {'Name': 'squawk_code', 'Type': 'string'},
                        {'Name': 'ground_speed', 'Type': 'int'},
                        {'Name': 'track', 'Type': 'int'},
                        {'Name': 'callsign', 'Type': 'string'},
                        {'Name': 'speed_knots', 'Type': 'double'},
                        {'Name': 'heading_degrees', 'Type': 'double'}
                    ],
                    'Location': 's3://flightradar-iceberg-data-dev-157809907894/radar_detections/',
                    'InputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergInputFormat',
                    'OutputFormat': 'org.apache.iceberg.mr.hive.HiveIcebergOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
                    }
                }
            }
        )
        
        print("✅ External table reference created")
        print("\n📋 AeroInsight users can now query: flightradar_external.radar_detections")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_cross_account_catalog()