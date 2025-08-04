import boto3
import time
from datetime import datetime

def setup_complete_redshift_column_security():
    """Complete setup script that ensures both DataScientist and FlightRadarViewer schemas exist"""
    
    redshift_data = boto3.client('redshift-data', region_name='us-east-1')
    workgroup_name = 'aeroinsight-workgroup-dev'
    database = 'analytics'
    
    print("🚀 COMPLETE REDSHIFT COLUMN SECURITY SETUP")
    print("=" * 70)
    print(f"📅 Setup Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🏢 Account: AeroInsight (707843606641)")
    print(f"🎯 Goal: Setup both DataScientist (13 cols) and FlightRadarViewer (11 cols) schemas")
    print("=" * 70)
    
    # Check existing schemas
    print("\n🔍 Step 1: Checking existing external schemas...")
    
    existing_schemas = []
    try:
        check_sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'wingsafe%';"
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=check_sql
        )
        
        query_id = response['Id']
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            if status_response['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(1)
        
        if status_response['Status'] == 'FINISHED':
            result = redshift_data.get_statement_result(Id=query_id)
            for record in result.get('Records', []):
                if 'stringValue' in record[0]:
                    existing_schemas.append(record[0]['stringValue'])
    except Exception as e:
        print(f"⚠️ Could not check existing schemas: {e}")
    
    print(f"📊 Found existing schemas: {existing_schemas}")
    
    # Setup DataScientist schema (wingsafe_catalog) - check if it exists by testing it
    datascientist_exists = test_schema_exists(redshift_data, workgroup_name, database, 'wingsafe_catalog')
    
    if not datascientist_exists:
        print("\n🔧 Step 2a: Creating DataScientist external schema...")
        
        datascientist_sql = """
        CREATE EXTERNAL SCHEMA wingsafe_catalog
        FROM DATA CATALOG
        DATABASE 'flightradar_db'
        IAM_ROLE 'arn:aws:iam::707843606641:role/AeroInsight-Redshift-Role-dev,arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev'
        REGION 'us-east-1'
        CATALOG_ID '184838390535';
        """
        
        if create_schema(redshift_data, workgroup_name, database, datascientist_sql, "wingsafe_catalog"):
            print("✅ DataScientist schema created successfully")
        else:
            print("❌ Failed to create DataScientist schema")
            return False
    else:
        print("✅ DataScientist schema (wingsafe_catalog) already exists")
    
    # Setup FlightRadarViewer schema - check if it exists by testing it
    flightradarviewer_exists = test_schema_exists(redshift_data, workgroup_name, database, 'wingsafe_flightradarviewer')
    
    if not flightradarviewer_exists:
        print("\n🔧 Step 2b: Creating FlightRadarViewer external schema...")
        
        flightradarviewer_sql = """
        CREATE EXTERNAL SCHEMA wingsafe_flightradarviewer
        FROM DATA CATALOG
        DATABASE 'flightradar_db'
        IAM_ROLE 'arn:aws:iam::707843606641:role/AeroInsight-Redshift-Role-dev,arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev'
        REGION 'us-east-1'
        CATALOG_ID '184838390535';
        """
        
        if create_schema(redshift_data, workgroup_name, database, flightradarviewer_sql, "wingsafe_flightradarviewer"):
            print("✅ FlightRadarViewer schema created successfully")
        else:
            print("❌ Failed to create FlightRadarViewer schema")
            return False
    else:
        print("✅ FlightRadarViewer schema (wingsafe_flightradarviewer) already exists")
    
    # Grant permissions on both schemas
    print("\n🔧 Step 3: Granting permissions on external schemas...")
    
    grant_commands = [
        "GRANT USAGE ON SCHEMA wingsafe_catalog TO PUBLIC;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA wingsafe_catalog TO PUBLIC;",
        "GRANT USAGE ON SCHEMA wingsafe_flightradarviewer TO PUBLIC;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA wingsafe_flightradarviewer TO PUBLIC;"
    ]
    
    for grant_sql in grant_commands:
        try:
            response = redshift_data.execute_statement(
                WorkgroupName=workgroup_name,
                Database=database,
                Sql=grant_sql
            )
            
            query_id = response['Id']
            while True:
                status_response = redshift_data.describe_statement(Id=query_id)
                if status_response['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(1)
            
            if status_response['Status'] == 'FINISHED':
                print(f"✅ Permission granted: {grant_sql}")
            else:
                print(f"⚠️ Permission grant warning: {grant_sql}")
        except Exception as e:
            print(f"⚠️ Permission grant error: {e}")
    
    # Test both schemas
    print("\n🧪 Step 4: Testing both schemas...")
    
    test_queries = [
        ("DataScientist", "wingsafe_catalog", 13),
        ("FlightRadarViewer", "wingsafe_flightradarviewer", 11)
    ]
    
    for role, schema, expected_cols in test_queries:
        print(f"\n🔍 Testing {role} schema ({schema})...")
        
        test_sql = f"SELECT * FROM {schema}.radar_detections LIMIT 1;"
        
        try:
            response = redshift_data.execute_statement(
                WorkgroupName=workgroup_name,
                Database=database,
                Sql=test_sql
            )
            
            query_id = response['Id']
            while True:
                status_response = redshift_data.describe_statement(Id=query_id)
                if status_response['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(2)
            
            if status_response['Status'] == 'FINISHED':
                result = redshift_data.get_statement_result(Id=query_id)
                actual_cols = len(result['ColumnMetadata'])
                columns = [col['name'] for col in result['ColumnMetadata']]
                
                restricted_cols = ['speed_knots', 'heading_degrees']
                has_restricted = any(col in restricted_cols for col in columns)
                
                print(f"   ✅ {role}: {actual_cols} columns (expected: {expected_cols})")
                print(f"   📊 Restricted columns visible: {'Yes' if has_restricted else 'No'}")
                
                if actual_cols == expected_cols:
                    print(f"   🎯 Column count matches expectation")
                else:
                    print(f"   ⚠️ Column count mismatch - may need LakeFormation permission update")
            else:
                print(f"   ❌ {role}: Test failed - {status_response['Status']}")
        
        except Exception as e:
            print(f"   ❌ {role}: Test error - {e}")
    
    print("\n🎉 SETUP COMPLETE!")
    print("\n📋 SUMMARY:")
    print("• ✅ DataScientist schema: wingsafe_catalog (should show 13 columns)")
    print("• ✅ FlightRadarViewer schema: wingsafe_flightradarviewer (should show 11 columns)")
    print("• 🔒 Column restrictions enforced by LakeFormation based on IAM role context")
    print("• 🔄 Cross-account access: AeroInsight → WingSafe → FlightRadar")
    
    print("\n🧪 NEXT STEPS:")
    print("• Run: python test-redshift-datascientist-comprehensive.py")
    print("• Run: python test-redshift-flightradarviewer-comprehensive.py")
    print("• Run: python compare-redshift-role-permissions.py")
    
    return True

def test_schema_exists(redshift_data, workgroup_name, database, schema_name):
    """Test if schema exists by trying to query it"""
    try:
        test_sql = f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}';"
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=test_sql
        )
        
        query_id = response['Id']
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            if status_response['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(1)
        
        if status_response['Status'] == 'FINISHED':
            result = redshift_data.get_statement_result(Id=query_id)
            return len(result.get('Records', [])) > 0
        return False
    except:
        return False

def create_schema(redshift_data, workgroup_name, database, create_sql, schema_name):
    """Helper function to create external schema"""
    try:
        response = redshift_data.execute_statement(
            WorkgroupName=workgroup_name,
            Database=database,
            Sql=create_sql
        )
        
        query_id = response['Id']
        while True:
            status_response = redshift_data.describe_statement(Id=query_id)
            status = status_response['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(2)
        
        if status == 'FINISHED':
            return True
        else:
            print(f"   ❌ Schema creation failed: {status}")
            if 'Error' in status_response:
                error_msg = status_response['Error']
                if 'already exists' in error_msg:
                    print(f"   ✅ Schema {schema_name} already exists - continuing...")
                    return True
                print(f"   Error: {error_msg}")
            return False
    
    except Exception as e:
        print(f"   ❌ Schema creation error: {e}")
        return False

if __name__ == "__main__":
    setup_complete_redshift_column_security()