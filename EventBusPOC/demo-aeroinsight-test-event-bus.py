#!/usr/bin/env python3
"""
Test script for Step 2: Test AeroInsight Lambda function only
Assumes DataScientist role and invokes Lambda to verify data writing and event publishing
"""

import boto3
import json
from botocore.exceptions import ClientError, NoCredentialsError

def assume_role_and_get_lambda_client(role_arn):
    """Assume cross-account role and return Lambda client"""
    sts = boto3.client('sts')
    
    try:
        print(f"Assuming role: {role_arn}")
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='AeroInsightLambdaTest'
        )
        
        credentials = response['Credentials']
        
        return boto3.client(
            'lambda',
            region_name='us-east-1',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    except ClientError as e:
        print(f"Failed to assume role: {e}")
        return None
    except NoCredentialsError:
        print("No AWS credentials found. Configure AWS CLI first.")
        return None

def test_lambda_function():
    """Test the AeroInsight Lambda function"""
    
    print("=" * 80)
    print("Step 2 Test: AeroInsight Lambda Data Writer")
    print("=" * 80)
    
    # Use AeroInsight account credentials directly (Lambda is in AeroInsight account)
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    
    print("Using AeroInsight account credentials")
    
    # Test Lambda function
    function_name = 'aero-data-writer-dev'
    
    try:
        print(f"Testing Lambda function: {function_name}")
        print("   Expected behavior:")
        print("   1. Lambda assumes DataScientist role internally")
        print("   2. Lambda writes 3 sample waypoints to aeronav_db.navigation_waypoints")
        print("   3. Lambda publishes event to EventBridge cross-account bus")
        print()
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        # Parse response
        payload = json.loads(response['Payload'].read())
        
        print(f"Lambda Response:")
        print(f"   Status Code: {response['StatusCode']}")
        
        if response['StatusCode'] == 200:
            print("Lambda invocation successful")
            
            if payload.get('statusCode') == 200:
                body = json.loads(payload['body'])
                print(f"\nLambda Execution Results:")
                print(f"   Status: SUCCESS")
                print(f"   Message: {body.get('message', 'No message')}")
                print(f"   Records Inserted: {body.get('recordsInserted', 'Unknown')}")
                print(f"   Query Execution ID: {body.get('queryExecutionId', 'Unknown')}")
                
                print(f"\nExpected Outcomes:")
                print(f"   Data written to Iceberg table successfully")
                print(f"   Event published to EventBridge bus")
                print(f"   Event should trigger FlightRadar Step Functions (if deployed)")
                
                print(f"\nVerification Steps:")
                print(f"   1. Check WingSafe Athena workgroup for query execution")
                print(f"   2. Query aeronav_db.navigation_waypoints for new POC records")
                print(f"   3. Check EventBridge metrics in WingSafe account")
                print(f"   4. Monitor FlightRadar Step Functions (if Step 3 deployed)")
                
                return True
                
            else:
                body = json.loads(payload['body'])
                print(f"\nLambda Execution Failed:")
                print(f"   Status Code: {payload.get('statusCode')}")
                print(f"   Error: {body.get('error', 'Unknown error')}")
                
                print(f"\nTroubleshooting:")
                print(f"   1. Check DataScientist role permissions")
                print(f"   2. Verify Iceberg table exists: aeronav_db.navigation_waypoints")
                print(f"   3. Check WingSafe workgroup: WingSafe-DataAnalysis-dev")
                print(f"   4. Verify S3 bucket: wingsafe-athena-results-dev-184838390535")
                
                return False
                
        else:
            print(f"Lambda invocation failed with status: {response['StatusCode']}")
            print(f"Response: {payload}")
            return False
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"Lambda function '{function_name}' not found")
            print("   Deploy Step 2 CloudFormation template first")
        elif error_code == 'AccessDeniedException':
            print(f"Access denied to invoke Lambda function")
            print("   Check DataScientist role permissions")
        else:
            print(f"AWS Error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False

def verify_prerequisites():
    """Verify prerequisites for the test"""
    print("Verifying Prerequisites:")
    
    try:
        # Check AWS credentials
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        account_id = identity.get('Account')
        print(f"   AWS Credentials: {identity.get('Arn', 'Unknown')}")
        
        # Verify we're in AeroInsight account (707843606641)
        if account_id != '707843606641':
            print(f"   Warning: Expected AeroInsight account (707843606641), got {account_id}")
            print(f"   Lambda function is in AeroInsight account")
        else:
            print(f"   AeroInsight account confirmed")
        
        # Check Lambda permissions
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        try:
            lambda_client.get_function(FunctionName='aero-data-writer-dev')
            print(f"   Lambda function accessible")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f"   Lambda function 'aero-data-writer-dev' not found")
            else:
                print(f"   Lambda access error: {e}")
            return False
            
        return True
        
    except Exception as e:
        print(f"   Prerequisites check failed: {e}")
        return False

if __name__ == "__main__":
    print("AeroInsight Lambda Test - Step 2 Verification")
    print("=" * 80)
    
    if not verify_prerequisites():
        print("\nPrerequisites not met. Fix issues above and retry.")
        exit(1)
    
    print()
    success = test_lambda_function()
    
    print("\n" + "=" * 80)
    if success:
        print("Step 2 Test PASSED: Lambda function working correctly")
        print("Ready to deploy and test Step 3 (FlightRadar Step Functions)")
    else:
        print("Step 2 Test FAILED: Fix Lambda function issues")
        print("Review CloudWatch logs and fix errors before proceeding")
    print("=" * 80)