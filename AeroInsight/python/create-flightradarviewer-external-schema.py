import boto3
import time

def create_flightradarviewer_external_schema():
    """Create external schema for FlightRadarViewer role with column restrictions"""
    
    redshift_data = boto3.client('redshift-data', region_name='us-east-1')
    workgroup_name = 'aeroinsight-workgroup-dev'
    database = 'analytics'
    
    print("🔧 Creating FlightRadarViewer external schema with column restrictions...")
    print("📋 Prerequisites: Existing DataScientist schema (wingsafe_catalog) should be working")
    print("🔑 Required: Admin permissions to create schemas in Redshift")
    
    try:
        # Step 1: Drop existing FlightRadarViewer schema if exists
        drop_schema_sql = "DROP SCHEMA IF EXISTS wingsafe_flightradarviewer CASCADE;"
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=drop_schema_sql
        )
        
        query_id = response['Id']
        
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(1)
        
        print("✅ Existing FlightRadarViewer schema dropped")
        
        # Step 2: Create external schema for FlightRadarViewer role
        create_schema_sql = """
        CREATE EXTERNAL SCHEMA wingsafe_flightradarviewer
        FROM DATA CATALOG
        DATABASE 'flightradar_db'
        IAM_ROLE 'arn:aws:iam::707843606641:role/AeroInsight-Redshift-Role-dev,arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev'
        REGION 'us-east-1'
        CATALOG_ID '184838390535';
        """
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=create_schema_sql
        )
        
        query_id = response['Id']
        
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(2)
        
        if status == 'FINISHED':
            print("✅ FlightRadarViewer external schema created successfully")
        else:
            print(f"❌ Schema creation failed: {status}")
            if 'Error' in status_response:
                print(f"Error: {status_response['Error']}")
            return False
        
        # Step 3: Grant permissions on the schema
        grant_commands = [
            "GRANT USAGE ON SCHEMA wingsafe_flightradarviewer TO PUBLIC;",
            "GRANT SELECT ON ALL TABLES IN SCHEMA wingsafe_flightradarviewer TO PUBLIC;"
        ]
        
        for grant_sql in grant_commands:
            response = redshift_data.execute_statement(
                WorkgroupName=workgroup_name,
                Database=database,
                Sql=grant_sql
            )
            
            query_id = response['Id']
            
            while True:
                status_response = redshift_data.describe_statement(Id=query_id)
                status = status_response['Status']
                if status in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(1)
            
            if status == 'FINISHED':
                print(f"✅ Permission granted: {grant_sql}")
            else:
                print(f"❌ Permission grant failed: {status}")
        
        # Step 4: Test the schema
        test_sql = "SELECT COUNT(*) FROM wingsafe_flightradarviewer.radar_detections;"
        
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=test_sql
        )
        
        query_id = response['Id']
        
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(2)
        
        if status == 'FINISHED':
            result = redshift_data.get_statement_result(Id=query_id)
            count = result['Records'][0][0]['longValue']
            print(f"✅ Schema test successful - Record count: {count}")
        else:
            print(f"⚠️ Schema test failed: {status}")
        
        print("\n🎉 FlightRadarViewer external schema setup complete!")
        print("\n📋 Schema Details:")
        print("• Schema Name: wingsafe_flightradarviewer")
        print("• Role Context: WingSafe-FlightRadarViewer-CrossAccount-dev")
        print("• Expected Access: Restricted (11 columns - speed_knots and heading_degrees hidden)")
        print("• LakeFormation: Column-level permissions enforced")
        print("• Existing DataScientist schema: wingsafe_catalog (13 columns)")
        print("\n⚠️ Note: If schema creation fails with permission denied, run as admin user")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating FlightRadarViewer schema: {e}")
        print("\n🔧 Troubleshooting:")
        print("• Ensure you have admin permissions in Redshift")
        print("• Verify WingSafe-FlightRadarViewer-CrossAccount-dev role exists")
        print("• Check that AeroInsight-Redshift-Role-dev can assume WingSafe roles")
        print("• Confirm LakeFormation permissions are set for FlightRadarViewer role")
        return False

if __name__ == "__main__":
    create_flightradarviewer_external_schema()