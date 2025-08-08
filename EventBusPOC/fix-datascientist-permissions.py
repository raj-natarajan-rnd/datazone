#!/usr/bin/env python3
"""
Fix DataScientist role Lake Formation permissions for INSERT operations
Run this from WingSafe account (184838390535)
"""

import boto3
from botocore.exceptions import ClientError

def list_databases_and_tables():
    """List available databases and tables"""
    glue = boto3.client('glue', region_name='us-east-1')
    
    print("🔍 Available databases:")
    databases = glue.get_databases()['DatabaseList']
    for db in databases:
        db_name = db['Name']
        print(f"   📁 {db_name}")
        
        try:
            tables = glue.get_tables(DatabaseName=db_name)['TableList']
            for table in tables:
                print(f"      📋 {table['Name']}")
        except Exception as e:
            print(f"      ❌ Error listing tables: {e}")
    
    return databases

def grant_table_permissions():
    """Grant INSERT/ALTER permissions to DataScientist role"""
    
    # First, list available databases and tables
    list_databases_and_tables()
    
    lakeformation = boto3.client('lakeformation', region_name='us-east-1')
    
    # DataScientist role ARN
    role_arn = 'arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev'
    
    # Try different possible database/table combinations
    possible_combinations = [
        ('aeronav_db', 'navigation_waypoints'),
        ('datalounge_aeronav_db', 'navigation_waypoints'),
        ('wingsafe_aeronav_db', 'navigation_waypoints')
    ]
    
    for db_name, table_name in possible_combinations:
        try:
            print(f"\n🔧 Trying {db_name}.{table_name}...")
            
            # Grant table permissions with INSERT/ALTER
            lakeformation.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': role_arn},
                Resource={
                    'Table': {
                        'DatabaseName': db_name,
                        'Name': table_name
                    }
                },
                Permissions=['INSERT', 'ALTER'],
                PermissionsWithGrantOption=[]
            )
        
            print(f"✅ INSERT/ALTER permissions granted for {db_name}.{table_name}")
            return True
            
        except ClientError as e:
            if 'Table not found' in str(e):
                print(f"⚠️ Table {db_name}.{table_name} not found")
                continue
            else:
                print(f"❌ Error: {e}")
                return False
    
    print("\n❌ No valid table found. Check database and table names.")
    return False

if __name__ == "__main__":
    print("🔧 Fixing DataScientist Lake Formation Permissions")
    print("=" * 60)
    
    success = grant_table_permissions()
    
    if success:
        print("\n✅ Permissions fixed! Lambda should now work.")
    else:
        print("\n❌ Failed to fix permissions.")