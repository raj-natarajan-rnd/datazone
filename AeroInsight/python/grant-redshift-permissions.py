import boto3
import time

def grant_schema_permissions():
    """Grant permissions on external schema to Redshift roles"""
    
    redshift_data = boto3.client('redshift-data', region_name='us-east-1')
    workgroup_name = 'aeroinsight-workgroup-dev'
    database = 'analytics'
    
    # Grant permissions to the current user and public
    grant_commands = [
        "GRANT USAGE ON SCHEMA wingsafe_catalog TO PUBLIC;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA wingsafe_catalog TO PUBLIC;"
    ]
    
    print("🔧 Granting permissions on external schema...")
    
    for i, sql in enumerate(grant_commands, 1):
        print(f"\n🔍 Command {i}: {sql}")
        
        try:
            response = redshift_data.execute_statement(
                WorkgroupName=workgroup_name,
                Database=database,
                Sql=sql
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
                print(f"✅ Permission granted successfully")
            else:
                print(f"❌ Grant failed: {status}")
                if 'Error' in status_response:
                    print(f"Error: {status_response['Error']}")
        
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print("-" * 60)

if __name__ == "__main__":
    grant_schema_permissions()