import boto3

def fix_quicksight_lakeformation_permissions():
    """Grant LakeFormation permissions for WingSafe cross-account roles used by QuickSight"""
    lakeformation = boto3.client('lakeformation')
    
    # Use the WingSafe cross-account roles that users assume
    roles = [
        "arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev",
        "arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev"
    ]
    
    try:
        print("🔧 Granting LakeFormation permissions for WingSafe roles...")
        
        for role_arn in roles:
            role_name = role_arn.split('/')[-1]
            
            # Grant database permissions
            try:
                lakeformation.grant_permissions(
                    Principal={'DataLakePrincipalIdentifier': role_arn},
                    Resource={'Database': {'Name': 'flightradar_db'}},
                    Permissions=['DESCRIBE']
                )
                print(f"✅ Database permissions granted to {role_name}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"✅ Database permissions already exist for {role_name}")
                else:
                    print(f"❌ Database permission error for {role_name}: {e}")
            
            # Grant table permissions
            try:
                lakeformation.grant_permissions(
                    Principal={'DataLakePrincipalIdentifier': role_arn},
                    Resource={
                        'Table': {
                            'DatabaseName': 'flightradar_db',
                            'Name': 'radar_detections'
                        }
                    },
                    Permissions=['SELECT', 'DESCRIBE']
                )
                print(f"✅ Table permissions granted to {role_name}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"✅ Table permissions already exist for {role_name}")
                else:
                    print(f"❌ Table permission error for {role_name}: {e}")
        
        print("\n🎉 WingSafe role LakeFormation permissions updated!")
        print("📋 QuickSight should now work when users switch to WingSafe roles")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing permissions: {e}")
        return False

if __name__ == "__main__":
    fix_quicksight_lakeformation_permissions()