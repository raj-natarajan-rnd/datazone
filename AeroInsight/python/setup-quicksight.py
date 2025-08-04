import boto3
import json
import time

def setup_quicksight():
    """Setup QuickSight with user groups and datasets for WingSafe data access"""
    quicksight = boto3.client('quicksight')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    
    try:
        print("🚀 Setting up QuickSight for WingSafe data access...")
        
        # Step 1: Create data source
        try:
            quicksight.create_data_source(
                AwsAccountId=account_id,
                DataSourceId='athena-datasource',
                Name='Athena Data Source',
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'primary'
                    }
                }
            )
            print("✅ Created QuickSight data source: athena-datasource")
        except Exception as e:
            if "already exists" in str(e):
                print("✅ Data source already exists: athena-datasource")
            else:
                print(f"❌ Failed to create data source: {e}")
        
        # Step 2: Create user groups
        groups = [
            {
                'GroupName': 'DataScientist-Group',
                'Description': 'Data scientists with full column access'
            },
            {
                'GroupName': 'FlightRadarViewer-Group', 
                'Description': 'Flight radar viewers with restricted column access'
            }
        ]
        
        for group in groups:
            try:
                quicksight.create_group(
                    AwsAccountId=account_id,
                    Namespace='default',
                    GroupName=group['GroupName'],
                    Description=group['Description']
                )
                print(f"✅ Created QuickSight group: {group['GroupName']}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"✅ Group already exists: {group['GroupName']}")
                else:
                    print(f"❌ Failed to create group {group['GroupName']}: {e}")
        
        # Step 3: Create dataset for radar detections
        dataset_id = 'radar-detections-dataset'
        
        try:
            quicksight.create_data_set(
                AwsAccountId=account_id,
                DataSetId=dataset_id,
                Name='Flight Radar Detections',
                ImportMode='DIRECT_QUERY',
                PhysicalTableMap={
                    'radartable': {
                        'RelationalTable': {
                            'DataSourceArn': f'arn:aws:quicksight:us-east-1:{account_id}:datasource/athena-datasource',
                            'Catalog': 'AwsDataCatalog',
                            'Schema': 'flightradar_db',
                            'Name': 'radar_detections',
                            'InputColumns': [
                                {'Name': 'timestamp', 'Type': 'DATETIME'},
                                {'Name': 'flight_id', 'Type': 'STRING'},
                                {'Name': 'aircraft_type', 'Type': 'STRING'},
                                {'Name': 'altitude_feet', 'Type': 'INTEGER'},
                                {'Name': 'latitude', 'Type': 'DECIMAL'},
                                {'Name': 'longitude', 'Type': 'DECIMAL'},
                                {'Name': 'vertical_speed', 'Type': 'INTEGER'},
                                {'Name': 'squawk_code', 'Type': 'STRING'},
                                {'Name': 'ground_speed', 'Type': 'INTEGER'},
                                {'Name': 'track', 'Type': 'INTEGER'},
                                {'Name': 'callsign', 'Type': 'STRING'},
                                {'Name': 'speed_knots', 'Type': 'DECIMAL'},
                                {'Name': 'heading_degrees', 'Type': 'DECIMAL'}
                            ]
                        }
                    }
                },
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:group/default/DataScientist-Group',
                        'Actions': [
                            'quicksight:DescribeDataSet',
                            'quicksight:DescribeDataSetPermissions',
                            'quicksight:PassDataSet',
                            'quicksight:DescribeIngestion',
                            'quicksight:ListIngestions'
                        ]
                    },
                    {
                        'Principal': f'arn:aws:quicksight:us-east-1:{account_id}:group/default/FlightRadarViewer-Group',
                        'Actions': [
                            'quicksight:DescribeDataSet',
                            'quicksight:DescribeDataSetPermissions',
                            'quicksight:PassDataSet',
                            'quicksight:DescribeIngestion',
                            'quicksight:ListIngestions'
                        ]
                    }
                ]
            )
            print(f"✅ Created QuickSight dataset: {dataset_id}")
        except Exception as e:
            if "already exists" in str(e):
                print(f"✅ Dataset already exists: {dataset_id}")
            else:
                print(f"❌ Failed to create dataset: {e}")
        
        print("\n🎉 QuickSight setup complete!")
        print("\n📋 Next steps:")
        print("• Assign SSO users to QuickSight groups")
        print("• Users must switch to WingSafe account roles to access data")
        print("• Create analyses and dashboards using the dataset in WingSafe account")
        print("• Test column-level restrictions based on assumed WingSafe roles")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting up QuickSight: {e}")
        return False

if __name__ == "__main__":
    setup_quicksight()