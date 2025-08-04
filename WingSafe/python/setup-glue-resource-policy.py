import boto3
import json

def setup_glue_resource_policy():
    """Setup Glue resource policy to allow FlightRadar cross-account access"""
    glue = boto3.client('glue')
    
    # Resource policy to allow FlightRadar account access
    resource_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::157809907894:root"
                },
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": [
                    "arn:aws:glue:us-east-1:184838390535:catalog",
                    "arn:aws:glue:us-east-1:184838390535:database/flightradar_db",
                    "arn:aws:glue:us-east-1:184838390535:table/flightradar_db/*"
                ]
            }
        ]
    }
    
    try:
        print("🔧 Setting up Glue resource policy for cross-account access...")
        
        # Set the resource policy
        glue.put_resource_policy(
            PolicyInJson=json.dumps(resource_policy)
        )
        
        print("✅ Glue resource policy set successfully")
        print("📋 FlightRadar account can now access WingSafe Glue catalog")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting Glue resource policy: {e}")
        return False

if __name__ == "__main__":
    setup_glue_resource_policy()