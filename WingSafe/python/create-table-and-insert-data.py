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

def setup_table_and_data():
    """Create Iceberg table and insert sample data from WingSafe centralized catalog"""
    
    # Step 1: Create Iceberg table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS flightradar_db.radar_detections (
        timestamp timestamp,
        flight_id string,
        aircraft_type string,
        altitude_feet int,
        latitude double,
        longitude double,
        vertical_speed int,
        squawk_code string,
        ground_speed int,
        track int,
        callsign string,
        speed_knots double,
        heading_degrees double
    )
    LOCATION 's3://flightradar-iceberg-data-dev-157809907894/radar_detections/'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet'
    )
    """
    
    if not execute_athena_query(create_table_query, "Creating Iceberg table"):
        return False
    
    # Step 2: Insert sample data
    insert_query = """
    INSERT INTO flightradar_db.radar_detections VALUES
    (timestamp '2024-01-15 10:30:00', 'FL001', 'Boeing 737', 35000, 40.7128, -74.0060, 0, '1200', 450, 90, 'UAL123', 450.5, 90.2),
    (timestamp '2024-01-15 10:31:00', 'FL002', 'Airbus A320', 32000, 40.7500, -73.9800, 500, '2000', 420, 85, 'DAL456', 420.8, 85.7),
    (timestamp '2024-01-15 10:32:00', 'FL003', 'Boeing 777', 38000, 40.6892, -74.0445, -200, '1000', 480, 95, 'AAL789', 480.2, 95.1),
    (timestamp '2024-01-15 10:33:00', 'FL004', 'Embraer E190', 28000, 40.7589, -73.9851, 800, '3000', 380, 75, 'JBU321', 380.9, 75.4),
    (timestamp '2024-01-15 10:34:00', 'FL005', 'Boeing 787', 41000, 40.6782, -74.0298, 0, '1200', 520, 100, 'SWA654', 520.1, 100.3)
    """
    
    if not execute_athena_query(insert_query, "Inserting sample data"):
        return False
    
    print("\n🎉 Table creation and data insertion complete!")
    print("\n📋 Centralized setup:")
    print("• Catalog: WingSafe account (184838390535)")
    print("• Data storage: FlightRadar S3 bucket (157809907894)")
    print("• Column permissions: WingSafe LakeFormation")
    print("• User access: AeroInsight account via cross-account roles")
    
    return True

if __name__ == "__main__":
    print("🚀 Setting up Iceberg table from WingSafe centralized catalog...")
    setup_table_and_data()