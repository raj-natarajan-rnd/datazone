import boto3
import time

def setup_redshift_external_schema():
    """Setup Redshift external schema to access WingSafe Glue catalog"""
    redshift_data = boto3.client('redshift-data')
    
    # Redshift cluster details
    workgroup_name = 'aeroinsight-workgroup-dev'
    database = 'analytics'
    
    try:
        print("🚀 Setting up Redshift external schema for WingSafe catalog access...")
        
        # Step 0: Drop existing schema if it exists
        drop_schema_sql = "DROP SCHEMA IF EXISTS wingsafe_catalog CASCADE;"
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=drop_schema_sql
        )
        
        query_id = response['Id']
        print(f"📋 Dropping existing schema query ID: {query_id}")
        
        # Wait for completion
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            
            time.sleep(2)
        
        if status == 'FINISHED':
            print("✅ Existing schema dropped successfully")
        else:
            print(f"⚠️ Schema drop status: {status}")
        
        # Step 1: Create external schema for WingSafe catalog
        create_schema_sql = """
        CREATE EXTERNAL SCHEMA wingsafe_catalog
        FROM DATA CATALOG
        DATABASE 'flightradar_db'
        IAM_ROLE 'arn:aws:iam::707843606641:role/AeroInsight-Redshift-Role-dev,arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev'
        REGION 'us-east-1'
        CATALOG_ID '184838390535';
        """
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=create_schema_sql
        )
        
        query_id = response['Id']
        print(f"📋 External schema creation query ID: {query_id}")
        
        # Wait for completion
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            
            print(f"Status: {status}")
            time.sleep(3)
        
        if status == 'FINISHED':
            print("✅ External schema created successfully")
        else:
            print(f"❌ External schema creation failed: {status}")
            if 'Error' in status_response:
                print(f"Error: {status_response['Error']}")
            return False
        
        # Step 2: Test query
        test_query = "SELECT COUNT(*) FROM wingsafe_catalog.radar_detections;"
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=test_query
        )
        
        query_id = response['Id']
        print(f"📋 Test query ID: {query_id}")
        
        # Wait for completion
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            
            time.sleep(3)
        
        if status == 'FINISHED':
            result = redshift_data.get_statement_result(Id=query_id)
            count = result['Records'][0][0]['longValue']
            print(f"✅ Test query successful - Record count: {count}")
        else:
            print(f"⚠️ Test query failed: {status}")
        
        print("\n🎉 Redshift external schema setup complete!")
        print("\n📋 Users can now:")
        print("• Access Redshift Query Editor in AeroInsight account")
        print("• Switch to WingSafe roles for data access")
        print("• Query: SELECT * FROM wingsafe_catalog.radar_detections LIMIT 10")
        print("• Column restrictions apply based on assumed WingSafe role")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting up Redshift external schema: {e}")
        return False

if __name__ == "__main__":
    setup_redshift_external_schema()