import boto3

def setup_lakeformation_permissions():
    """Setup LakeFormation column-level permissions for different user types"""
    lakeformation = boto3.client('lakeformation')
    
    # Role ARNs
    datascientist_role = "arn:aws:iam::184838390535:role/WingSafe-DataScientist-CrossAccount-dev"
    flightradar_viewer_role = "arn:aws:iam::184838390535:role/WingSafe-FlightRadarViewer-CrossAccount-dev"
    
    try:
        print("🔧 Setting up LakeFormation column-level permissions...")
        
        # Step 1: Revoke IAM_ALLOWED_PRINCIPALS if exists
        print("1. Checking IAM_ALLOWED_PRINCIPALS permissions...")
        try:
            permissions = lakeformation.list_permissions(
                Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}
            )
            
            if permissions['PrincipalResourcePermissions']:
                print("Found IAM_ALLOWED_PRINCIPALS - revoking...")
                lakeformation.revoke_permissions(
                    Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'},
                    Resource={
                        'Table': {
                            'CatalogId': '184838390535',
                            'DatabaseName': 'flightradar_db',
                            'Name': 'radar_detections'
                        }
                    },
                    Permissions=['ALL']
                )
                print("✅ IAM_ALLOWED_PRINCIPALS revoked")
            else:
                print("✅ No IAM_ALLOWED_PRINCIPALS found")
        except Exception as e:
            print(f"⚠️ Manual revoke may be needed: {e}")
        
        # Step 2: Grant database permissions to both roles
        print("2. Granting database permissions...")
        for role_arn in [datascientist_role, flightradar_viewer_role]:
            lakeformation.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': role_arn},
                Resource={
                    'Database': {
                        'CatalogId': '184838390535',
                        'Name': 'flightradar_db'
                    }
                },
                Permissions=['DESCRIBE']
            )
        print("✅ Database permissions granted to both roles")
        
        # Step 3: Grant FULL table access to DataScientist role (all columns)
        print("3. Granting FULL access to DataScientist role...")
        lakeformation.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': datascientist_role},
            Resource={
                'Table': {
                    'CatalogId': '184838390535',
                    'DatabaseName': 'flightradar_db',
                    'Name': 'radar_detections'
                }
            },
            Permissions=['SELECT']
        )
        print("✅ DataScientist: FULL access to all columns")
        
        # Step 4: Grant RESTRICTED column access to FlightRadar Viewer role
        print("4. Granting RESTRICTED access to FlightRadar Viewer role...")
        allowed_columns = [
            'timestamp', 'flight_id', 'aircraft_type', 'altitude_feet',
            'latitude', 'longitude', 'vertical_speed', 'squawk_code',
            'ground_speed', 'track', 'callsign'
        ]
        
        lakeformation.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': flightradar_viewer_role},
            Resource={
                'TableWithColumns': {
                    'CatalogId': '184838390535',
                    'DatabaseName': 'flightradar_db',
                    'Name': 'radar_detections',
                    'ColumnNames': allowed_columns
                }
            },
            Permissions=['SELECT']
        )
        print("✅ FlightRadar Viewer: RESTRICTED access (excluded: speed_knots, heading_degrees)")
        
        print("\n🎉 LakeFormation permissions setup complete!")
        print("\n📋 Summary:")
        print("• DataScientist users: Full access to all columns")
        print("• FlightRadar Viewer users: Cannot see speed_knots, heading_degrees")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    setup_lakeformation_permissions()