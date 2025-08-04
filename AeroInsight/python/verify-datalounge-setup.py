import boto3

def verify_datalounge_setup():
    """Verify DataLounge multi-application setup is working correctly"""
    
    print("DATALOUNGE SETUP VERIFICATION")
    print("=" * 80)
    
    # Initialize clients
    glue = boto3.client('glue')
    
    # Expected setup
    expected_databases = ['aeronav_db', 'aeroweather_db', 'aerotraffic_db']
    expected_tables = {
        'aeronav_db': ['navigation_waypoints', 'flight_routes'],
        'aeroweather_db': ['weather_observations', 'weather_forecasts'],
        'aerotraffic_db': ['air_traffic_control', 'runway_operations']
    }
    
    verification_results = []
    
    print("\n🔍 Checking Glue Catalog Registration...")
    
    # Check databases
    try:
        databases = glue.get_databases()
        found_databases = [db['Name'] for db in databases['DatabaseList']]
        
        for db_name in expected_databases:
            if db_name in found_databases:
                verification_results.append(f"✅ Database {db_name}: Found")
                
                # Check tables in database
                try:
                    tables = glue.get_tables(DatabaseName=db_name)
                    found_tables = [table['Name'] for table in tables['TableList']]
                    
                    for table_name in expected_tables[db_name]:
                        if table_name in found_tables:
                            verification_results.append(f"✅ Table {db_name}.{table_name}: Found")
                            
                            # Check if it's an Iceberg table
                            table_details = glue.get_table(DatabaseName=db_name, Name=table_name)
                            table_type = table_details['Table'].get('Parameters', {}).get('table_type', 'Unknown')
                            
                            if table_type.upper() == 'ICEBERG':
                                verification_results.append(f"✅ {db_name}.{table_name}: Iceberg table")
                            else:
                                verification_results.append(f"❌ {db_name}.{table_name}: Not Iceberg ({table_type})")
                        else:
                            verification_results.append(f"❌ Table {db_name}.{table_name}: Missing")
                            
                except Exception as e:
                    verification_results.append(f"❌ Error checking tables in {db_name}: {str(e)[:50]}")
            else:
                verification_results.append(f"❌ Database {db_name}: Missing")
                
    except Exception as e:
        verification_results.append(f"❌ Error accessing Glue catalog: {str(e)[:50]}")
    
    # Display results
    print("\nVERIFICATION RESULTS:")
    print("=" * 80)
    
    for result in verification_results:
        print(result)
    
    # Summary
    ok_count = len([r for r in verification_results if r.startswith('✅')])
    error_count = len([r for r in verification_results if r.startswith('❌')])
    total_count = len(verification_results)
    
    print(f"\nSUMMARY:")
    print(f"✅ OK: {ok_count}/{total_count}")
    print(f"❌ Errors: {error_count}/{total_count}")
    
    if error_count == 0:
        print("\n🎉 VERIFICATION PASSED! DataLounge setup is ready for demo.")
        print("\nNext steps:")
        print("1. Run: python demo-datalounge-multi-application.py")
        print("2. Test different role contexts via SSO permission sets")
        print("3. Verify column-level security enforcement")
    else:
        print("\n⚠️ VERIFICATION ISSUES FOUND!")
        print("\nTroubleshooting:")
        print("1. Check CloudFormation stack deployments")
        print("2. Verify LakeFormation permissions setup")
        print("3. Ensure cross-account roles are properly configured")
        print("4. Review deployment guide: DATALOUNGE_DEPLOYMENT_STEPS.md")

if __name__ == "__main__":
    verify_datalounge_setup()