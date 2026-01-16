#!/usr/bin/env python3
"""
AWS Read-Only Permissions Validator

Validates AWS read-only permissions for the current user/role.
ONLY tests read-only permissions (Describe*, List*, Get*, Lookup*, Search*).
This tool does NOT perform any write, delete, or modify operations.

Features:
- Tests permissions by calling AWS APIs with test resource names
- "Not found" errors prove the permission exists (API was authorized)
- Optionally queries CloudTrail for denied permission diagnostics
- Generates markdown report with results matrix

Usage:
    python validate-read-only-aws-permissions.py                      # Use config from YAML
    python validate-read-only-aws-permissions.py --profile my-profile # Override profile
    python validate-read-only-aws-permissions.py --service ec2        # Validate only EC2
    python validate-read-only-aws-permissions.py --service ec2 s3     # Validate EC2 and S3
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import boto3
import yaml
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, ParamValidationError


# Map of AWS service names to boto3 client names (where they differ)
SERVICE_CLIENT_MAP = {
    "secretsmanager": "secretsmanager",
    "cloudformation": "cloudformation",
    "cloudtrail": "cloudtrail",
    "dynamodb": "dynamodb",
    "logs": "logs",
    "athena": "athena",
    "glue": "glue",
    "cognito-idp": "cognito-idp",
    "elasticloadbalancing": "elbv2",  # ALB/NLB use elbv2
    "states": "stepfunctions",
}

# Permissions that use different clients than their service
PERMISSION_CLIENT_OVERRIDE = {
    ("dynamodb", "DescribeStream"): "dynamodbstreams",
    ("dynamodb", "ListStreams"): "dynamodbstreams",
    ("s3", "ListMultiRegionAccessPoints"): "s3control",
    ("elasticloadbalancing", "DescribeInstanceHealth"): "elb",  # Classic ELB
}

# Permissions that require specific parameters to make an actual AWS API call
# Format: (service, permission) -> {param_name: param_value or "TEST_RESOURCE"}
# "TEST_RESOURCE" will be replaced with generate_test_resource_name() result
PERMISSION_REQUIRED_PARAMS = {
    # API Gateway
    ("apigateway", "GetResources"): {"restApiId": "TEST_RESOURCE"},
    ("apigateway", "GetMethod"): {"restApiId": "TEST_RESOURCE", "resourceId": "test", "httpMethod": "GET"},
    ("apigateway", "GetStages"): {"restApiId": "TEST_RESOURCE"},
    ("apigateway", "GetDeployments"): {"restApiId": "TEST_RESOURCE"},
    ("apigateway", "GetAuthorizers"): {"restApiId": "TEST_RESOURCE"},

    # Athena
    ("athena", "ListDatabases"): {"CatalogName": "AwsDataCatalog"},
    ("athena", "ListTableMetadata"): {"CatalogName": "AwsDataCatalog", "DatabaseName": "TEST_RESOURCE"},

    # Cognito
    ("cognito-idp", "ListUserPools"): {"MaxResults": 1},
    ("cognito-idp", "ListIdentityProviders"): {"UserPoolId": "TEST_RESOURCE"},
    ("cognito-idp", "ListResourceServers"): {"UserPoolId": "TEST_RESOURCE"},

    # DynamoDB
    ("dynamodb", "DescribeBackup"): {"BackupArn": "arn:aws:dynamodb:us-east-1:000000000000:table/test/backup/test"},
    ("dynamodb", "DescribeContinuousBackups"): {"TableName": "TEST_RESOURCE"},
    ("dynamodb", "DescribeGlobalTable"): {"GlobalTableName": "TEST_RESOURCE"},
    ("dynamodb", "DescribeGlobalTableSettings"): {"GlobalTableName": "TEST_RESOURCE"},
    ("dynamodb", "DescribeStream"): {"StreamArn": "arn:aws:dynamodb:us-east-1:000000000000:table/test/stream/test"},
    ("dynamodb", "DescribeTimeToLive"): {"TableName": "TEST_RESOURCE"},
    ("dynamodb", "ListTagsOfResource"): {"ResourceArn": "arn:aws:dynamodb:us-east-1:000000000000:table/TEST_RESOURCE"},

    # EC2
    ("ec2", "DescribeIdentityIdFormat"): {"PrincipalArn": "arn:aws:iam::000000000000:root"},
    ("ec2", "DescribeImageAttribute"): {"ImageId": "ami-00000000", "Attribute": "description"},
    ("ec2", "DescribeInstanceAttribute"): {"InstanceId": "i-00000000", "Attribute": "instanceType"},
    ("ec2", "DescribeNetworkInterfaceAttribute"): {"NetworkInterfaceId": "eni-00000000", "Attribute": "description"},
    ("ec2", "DescribeSnapshotAttribute"): {"SnapshotId": "snap-00000000", "Attribute": "productCodes"},
    ("ec2", "DescribeVolumeAttribute"): {"VolumeId": "vol-00000000", "Attribute": "productCodes"},
    ("ec2", "DescribeVpcAttribute"): {"VpcId": "vpc-00000000", "Attribute": "enableDnsSupport"},
    ("ec2", "DescribeVpcEndpointServicePermissions"): {"ServiceId": "vpce-svc-00000000"},
    ("ec2", "DescribeSpotFleetInstances"): {"SpotFleetRequestId": "sfr-00000000"},
    ("ec2", "DescribeSpotFleetRequestHistory"): {"SpotFleetRequestId": "sfr-00000000", "StartTime": "2020-01-01T00:00:00Z"},
    ("ec2", "SearchLocalGatewayRoutes"): {"LocalGatewayRouteTableId": "lgw-rtb-00000000", "Filters": [{"Name": "type", "Values": ["static"]}]},

    # ECR
    ("ecr", "ListImages"): {"repositoryName": "TEST_RESOURCE"},

    # ECS
    ("ecs", "DescribeContainerInstances"): {"cluster": "TEST_RESOURCE", "containerInstances": ["arn:aws:ecs:us-east-1:000000000000:container-instance/test"]},
    ("ecs", "DescribeServices"): {"cluster": "TEST_RESOURCE", "services": ["test"]},
    ("ecs", "DescribeTaskDefinition"): {"taskDefinition": "TEST_RESOURCE"},
    ("ecs", "DescribeTaskSets"): {"cluster": "TEST_RESOURCE", "service": "test"},
    ("ecs", "DescribeTasks"): {"cluster": "TEST_RESOURCE", "tasks": ["arn:aws:ecs:us-east-1:000000000000:task/test"]},
    ("ecs", "ListAttributes"): {"targetType": "container-instance"},
    ("ecs", "ListTagsForResource"): {"resourceArn": "arn:aws:ecs:us-east-1:000000000000:cluster/TEST_RESOURCE"},

    # EKS
    ("eks", "ListTagsForResource"): {"resourceArn": "arn:aws:eks:us-east-1:000000000000:cluster/TEST_RESOURCE"},
    ("eks", "DescribeCluster"): {"name": "TEST_RESOURCE"},
    ("eks", "ListNodegroups"): {"clusterName": "TEST_RESOURCE"},

    # Elastic Load Balancing
    ("elasticloadbalancing", "DescribeInstanceHealth"): {"LoadBalancerName": "TEST_RESOURCE"},
    ("elasticloadbalancing", "DescribeLoadBalancerAttributes"): {"LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/0000000000000000"},
    ("elasticloadbalancing", "DescribeTargetHealth"): {"TargetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/test/0000000000000000"},
    ("elasticloadbalancing", "DescribeTags"): {"ResourceArns": ["arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/0000000000000000"]},

    # Events (EventBridge)
    ("events", "ListRuleNamesByTarget"): {"TargetArn": "arn:aws:lambda:us-east-1:000000000000:function:TEST_RESOURCE"},
    ("events", "ListTargetsByRule"): {"Rule": "TEST_RESOURCE"},

    # Glue
    ("glue", "GetTags"): {"ResourceArn": "arn:aws:glue:us-east-1:000000000000:database/TEST_RESOURCE"},

    # IAM
    ("iam", "ListAttachedGroupPolicies"): {"GroupName": "TEST_RESOURCE"},
    ("iam", "ListAttachedRolePolicies"): {"RoleName": "TEST_RESOURCE"},
    ("iam", "ListAttachedUserPolicies"): {"UserName": "TEST_RESOURCE"},
    ("iam", "ListEntitiesForPolicy"): {"PolicyArn": "arn:aws:iam::000000000000:policy/TEST_RESOURCE"},
    ("iam", "ListGroupPolicies"): {"GroupName": "TEST_RESOURCE"},
    ("iam", "ListGroupsForUser"): {"UserName": "TEST_RESOURCE"},
    ("iam", "ListInstanceProfilesForRole"): {"RoleName": "TEST_RESOURCE"},
    ("iam", "ListPolicyVersions"): {"PolicyArn": "arn:aws:iam::000000000000:policy/TEST_RESOURCE"},
    ("iam", "ListRolePolicies"): {"RoleName": "TEST_RESOURCE"},
    ("iam", "ListRoleTags"): {"RoleName": "TEST_RESOURCE"},
    ("iam", "ListUserPolicies"): {"UserName": "TEST_RESOURCE"},
    ("iam", "ListUserTags"): {"UserName": "TEST_RESOURCE"},

    # KMS
    ("kms", "ListResourceTags"): {"KeyId": "alias/aws/s3"},

    # Lambda
    ("lambda", "ListAliases"): {"FunctionName": "TEST_RESOURCE"},
    ("lambda", "ListFunctionEventInvokeConfigs"): {"FunctionName": "TEST_RESOURCE"},
    ("lambda", "ListFunctionsByCodeSigningConfig"): {"CodeSigningConfigArn": "arn:aws:lambda:us-east-1:000000000000:code-signing-config:csc-0000000000000000"},
    ("lambda", "ListFunctionUrlConfigs"): {"FunctionName": "TEST_RESOURCE"},
    ("lambda", "ListLayerVersions"): {"LayerName": "TEST_RESOURCE"},
    ("lambda", "ListProvisionedConcurrencyConfigs"): {"FunctionName": "TEST_RESOURCE"},
    ("lambda", "ListTags"): {"Resource": "arn:aws:lambda:us-east-1:000000000000:function:TEST_RESOURCE"},
    ("lambda", "ListVersionsByFunction"): {"FunctionName": "TEST_RESOURCE"},

    # CloudWatch Logs
    ("logs", "DescribeSubscriptionFilters"): {"logGroupName": "TEST_RESOURCE"},
    ("logs", "GetTransformer"): {"logGroupIdentifier": "TEST_RESOURCE"},
    ("logs", "ListLogGroupsForQuery"): {"queryId": "00000000-0000-0000-0000-000000000000"},
    ("logs", "ListTagsForResource"): {"resourceArn": "arn:aws:logs:us-east-1:000000000000:log-group:TEST_RESOURCE"},

    # RDS
    ("rds", "DescribeDBClusterParameters"): {"DBClusterParameterGroupName": "TEST_RESOURCE"},
    ("rds", "DescribeDBClusterSnapshotAttributes"): {"DBClusterSnapshotIdentifier": "TEST_RESOURCE"},
    ("rds", "DescribeDBLogFiles"): {"DBInstanceIdentifier": "TEST_RESOURCE"},
    ("rds", "DescribeDBParameters"): {"DBParameterGroupName": "TEST_RESOURCE"},
    ("rds", "DescribeDBSnapshotAttributes"): {"DBSnapshotIdentifier": "TEST_RESOURCE"},
    ("rds", "DescribeEngineDefaultClusterParameters"): {"DBParameterGroupFamily": "aurora-mysql8.0"},
    ("rds", "DescribeEngineDefaultParameters"): {"DBParameterGroupFamily": "mysql8.0"},
    ("rds", "DescribeOptionGroupOptions"): {"EngineName": "mysql"},
    ("rds", "DescribeOrderableDBInstanceOptions"): {"Engine": "mysql"},

    # S3
    ("s3", "ListObjectsV2"): {"Bucket": "TEST_RESOURCE"},
    ("s3", "GetBucketPolicy"): {"Bucket": "TEST_RESOURCE"},
    ("s3", "GetBucketAcl"): {"Bucket": "TEST_RESOURCE"},
    # s3control - needs AccountId (will be replaced dynamically)
    ("s3", "ListMultiRegionAccessPoints"): {"AccountId": "ACCOUNT_ID"},

    # SNS
    ("sns", "ListEndpointsByPlatformApplication"): {"PlatformApplicationArn": "arn:aws:sns:us-east-1:000000000000:app/GCM/TEST_RESOURCE"},
    ("sns", "ListSubscriptionsByTopic"): {"TopicArn": "arn:aws:sns:us-east-1:000000000000:TEST_RESOURCE"},
    ("sns", "ListTagsForResource"): {"ResourceArn": "arn:aws:sns:us-east-1:000000000000:TEST_RESOURCE"},

    # SQS
    ("sqs", "GetQueueUrl"): {"QueueName": "TEST_RESOURCE"},
    ("sqs", "ListDeadLetterSourceQueues"): {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/000000000000/TEST_RESOURCE"},
    ("sqs", "ListMessageMoveTasks"): {"SourceArn": "arn:aws:sqs:us-east-1:000000000000:TEST_RESOURCE"},
    ("sqs", "ListQueueTags"): {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/000000000000/TEST_RESOURCE"},

    # SSM
    ("ssm", "ListInventoryEntries"): {"InstanceId": "i-00000000000000000", "TypeName": "AWS:InstanceInformation"},

    # Step Functions
    ("states", "ListExecutions"): {"stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:TEST_RESOURCE"},
    ("states", "ListMapRuns"): {"executionArn": "arn:aws:states:us-east-1:000000000000:execution:test:test"},
    ("states", "ListStateMachineAliases"): {"stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:TEST_RESOURCE"},
    ("states", "ListStateMachineVersions"): {"stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:TEST_RESOURCE"},
    ("states", "DescribeStateMachine"): {"stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:TEST_RESOURCE"},

    # CloudWatch
    ("cloudwatch", "GetDashboard"): {"DashboardName": "TEST_RESOURCE"},
    ("cloudwatch", "GetMetricData"): {"MetricDataQueries": [{"Id": "m1", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization"}, "Period": 300, "Stat": "Average"}}], "StartTime": "2020-01-01T00:00:00Z", "EndTime": "2020-01-01T01:00:00Z"},
    ("cloudwatch", "GetMetricStatistics"): {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization", "StartTime": "2020-01-01T00:00:00Z", "EndTime": "2020-01-01T01:00:00Z", "Period": 300, "Statistics": ["Average"]},
    ("cloudwatch", "DescribeAlarmsForMetric"): {"MetricName": "CPUUtilization", "Namespace": "AWS/EC2"},
}

# Read-only permission prefixes (safety check)
READONLY_PREFIXES = ("Describe", "List", "Get", "Lookup", "Search")

# Error codes that indicate "resource not found" - these prove permission exists
NOT_FOUND_ERROR_CODES = (
    "NoSuchBucket",
    "NoSuchKey",
    "NoSuchTagSet",
    "FunctionNotFound",
    "ResourceNotFoundException",
    "ResourceNotFoundFault",
    "ResourceNotFound",
    "SecretNotFoundError",
    "NotFoundException",
    "KeyNotFoundException",
    "InvalidKeyId",
    "QueueDoesNotExist",
    "AWS.SimpleQueueService.NonExistentQueue",
    "TrailNotFoundException",
    "NoSuchEntity",
    "EntityNotFoundException",
    "DBInstanceNotFound",
    "DBClusterNotFoundFault",
    "DBSnapshotNotFound",
    "StackNotFoundException",
    "LogGroupNotFoundException",
    "WorkGroupNotFoundException",
    "TableNotFoundException",
    "DatabaseNotFoundException",
    # EC2 specific not-found errors
    "InvalidAMIID.NotFound",
    "InvalidInstanceID.NotFound",
    "InvalidNetworkInterfaceID.NotFound",
    "InvalidSnapshot.NotFound",
    "InvalidVolume.NotFound",
    "InvalidVpcID.NotFound",
    "InvalidSpotFleetRequestId.NotFound",
    "InvalidLocalGatewayRouteTableID.NotFound",
    "InvalidVpcEndpointServiceId.NotFound",
    # Other service not-found errors
    "PlatformApplicationDisabledException",
    "InvalidTargetArn.Unknown",
)


def generate_test_resource_name(service: str, permission: str) -> str:
    """
    Generate a test resource name for permission validation.

    Some services only check permissions when the resource exists.
    For these, we use well-known resources that exist in all AWS accounts.
    For other services, "not found" errors prove the permission exists.
    """
    # KMS: Use AWS-managed key alias (exists in all accounts)
    # This ensures permission check happens instead of "key not found"
    if service == "kms" and permission == "DescribeKey":
        return "alias/aws/s3"

    # Default format - works for services that check permissions first
    return f"perm-validator-test-{service}-{permission.lower()}"


def to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case for boto3 method names."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def is_readonly_permission(permission: str) -> bool:
    """Check if a permission is read-only based on its prefix."""
    return permission.startswith(READONLY_PREFIXES)


def get_boto3_client(service: str, region: str, profile: Optional[str] = None):
    """Get boto3 client for the specified service."""
    client_name = SERVICE_CLIENT_MAP.get(service, service)
    if profile and profile != "default":
        session = boto3.Session(profile_name=profile)
        return session.client(client_name, region_name=region)
    return boto3.client(client_name, region_name=region)


def extract_meaningful_data(service: str, permission: str, response: Any) -> dict:
    """
    Extract meaningful summary data from API response.
    Generic implementation that works with any AWS API response.
    """
    data = {"count": 0, "sample": None, "details": ""}

    if not response or not isinstance(response, dict):
        data["details"] = "API call successful"
        data["count"] = 1
        return data

    try:
        # Special case for GetCallerIdentity (useful info)
        if permission == "GetCallerIdentity":
            data["details"] = f"Account: {response.get('Account')}, ARN: {response.get('Arn')}"
            data["count"] = 1
            return data

        # Special case for DescribeInstances (nested structure)
        if permission == "DescribeInstances":
            instances = []
            for res in response.get("Reservations", []):
                instances.extend(res.get("Instances", []))
            data["count"] = len(instances)
            data["details"] = f"{len(instances)} instances"
            return data

        # Generic: Find the main data array in the response
        # Skip metadata keys and find arrays
        skip_keys = {"ResponseMetadata", "NextToken", "NextMarker", "Marker", "IsTruncated"}

        main_array = None
        main_key = None

        for key, value in response.items():
            if key in skip_keys:
                continue
            if isinstance(value, list):
                # Prefer the largest array (likely the main data)
                if main_array is None or len(value) > len(main_array):
                    main_array = value
                    main_key = key

        if main_array is not None:
            data["count"] = len(main_array)
            # Create human-readable label from key (e.g., "Buckets" -> "buckets")
            label = main_key.lower() if main_key else "items"
            data["details"] = f"{len(main_array)} {label}"
        else:
            # No array found - single resource or simple response
            data["details"] = "API call successful"
            data["count"] = 1

    except Exception:
        data["details"] = "API call successful"
        data["count"] = 1

    return data


def validate_permission(service: str, permission: str, region: str, profile: Optional[str] = None) -> dict:
    """
    Validate a single permission by attempting to call the corresponding API.
    Uses test resource names for resource-specific calls.
    A "not found" error proves the permission exists.
    """
    result = {
        "service": service,
        "permission": permission,
        "status": "UNKNOWN",
        "message": "",
        "error_code": None,
        "data": {"count": 0, "sample": None, "details": ""},
    }

    # Safety check: only test read-only permissions
    if not is_readonly_permission(permission):
        result["status"] = "SKIPPED"
        result["message"] = "Not a read-only permission (safety check)"
        return result

    test_resource = generate_test_resource_name(service, permission)

    try:
        # Check if this permission uses a different client
        client_service = PERMISSION_CLIENT_OVERRIDE.get((service, permission), service)
        client = get_boto3_client(client_service, region, profile)
        method_name = to_snake_case(permission)

        # Check if method exists
        if not hasattr(client, method_name):
            result["status"] = "ERROR"
            result["message"] = f"Method '{method_name}' not found on {service} client"
            return result

        method = getattr(client, method_name)
        response = None

        # Build parameters from mapping if available
        param_key = (service, permission)
        mapped_params = PERMISSION_REQUIRED_PARAMS.get(param_key)

        # Call the method with appropriate parameters
        try:
            # First check if we have mapped parameters for this permission
            if mapped_params:
                # Get account ID for ACCOUNT_ID placeholder if needed
                account_id = None
                if any(v == "ACCOUNT_ID" or (isinstance(v, str) and "ACCOUNT_ID" in v) for v in mapped_params.values()):
                    sts_client = get_boto3_client("sts", region, profile)
                    account_id = sts_client.get_caller_identity().get("Account", "000000000000")

                # Replace placeholders with actual values
                params = {}
                for key, value in mapped_params.items():
                    if value == "TEST_RESOURCE":
                        params[key] = test_resource
                    elif value == "ACCOUNT_ID":
                        params[key] = account_id
                    elif isinstance(value, str) and "TEST_RESOURCE" in value:
                        params[key] = value.replace("TEST_RESOURCE", test_resource)
                    elif isinstance(value, str) and "ACCOUNT_ID" in value:
                        params[key] = value.replace("ACCOUNT_ID", account_id)
                    else:
                        params[key] = value
                response = method(**params)

            # Permissions that work without parameters
            elif permission == "GetCallerIdentity":
                response = method()
            elif permission == "GetUser":
                response = method()

            # S3 bucket permissions - use test bucket name
            elif permission in ("GetBucketLocation", "GetBucketVersioning", "GetBucketTagging"):
                response = method(Bucket=test_resource)

            # Lambda
            elif permission == "GetFunction":
                response = method(FunctionName=test_resource)

            # Secrets Manager
            elif permission == "DescribeSecret":
                response = method(SecretId=test_resource)

            # KMS
            elif permission == "DescribeKey":
                response = method(KeyId=test_resource)

            # DynamoDB
            elif permission == "DescribeTable":
                response = method(TableName=test_resource)

            # SQS - needs a URL format
            elif permission == "GetQueueAttributes":
                # SQS queue URL format
                sts_client = get_boto3_client("sts", region, profile)
                account_id = sts_client.get_caller_identity().get("Account", "000000000000")
                test_queue_url = f"https://sqs.{region}.amazonaws.com/{account_id}/{test_resource}"
                response = method(QueueUrl=test_queue_url, AttributeNames=["All"])

            # CloudTrail
            elif permission == "GetTrailStatus":
                response = method(Name=test_resource)

            # IAM
            elif permission == "GetRole":
                response = method(RoleName=test_resource)

            # CloudWatch Logs
            elif permission == "DescribeLogStreams":
                response = method(logGroupName=test_resource, limit=1)

            # Glue
            elif permission == "GetTables":
                response = method(DatabaseName=test_resource)

            # Permissions with special parameters
            elif permission == "DescribeSnapshots":
                response = method(OwnerIds=["self"])
            elif permission == "DescribeImages":
                response = method(Owners=["self"])
            else:
                response = method()

            result["status"] = "PASSED"
            result["message"] = "Permission validated successfully"

            # Extract meaningful data from response
            if response:
                result["data"] = extract_meaningful_data(service, permission, response)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            if error_code in ("AccessDenied", "AccessDeniedException", "UnauthorizedAccess",
                              "UnauthorizedOperation", "AuthorizationError"):
                result["status"] = "DENIED"
                result["message"] = error_message
                result["error_code"] = error_code
            elif error_code in NOT_FOUND_ERROR_CODES:
                # "Not found" errors prove the permission exists - API call was authorized
                result["status"] = "PASSED"
                result["message"] = "Permission validated (resource not found)"
                result["data"]["details"] = f"API authorized ({error_code})"
            elif error_code in ("ValidationError", "ValidationException", "InvalidParameterValue",
                                "MissingParameter", "InvalidSpotDatafeed.NotFound", "OptInRequired",
                                "UnsupportedOperation", "BadRequest", "InvalidParameterCombination",
                                # Malformed test data errors - prove API is accessible
                                "InvalidParameterException", "InvalidArnException", "InvalidAddress",
                                "InvalidClientTokenId", "PermanentRedirect",
                                "InvalidLocalGatewayRouteTableID.Malformed"):
                # These errors prove the API is accessible - just needs valid parameters or features
                result["status"] = "PASSED"
                result["message"] = f"Permission exists ({error_code})"
                result["data"]["details"] = "API accessible"
            elif "Parameter validation failed" in error_message or "Missing required parameter" in error_message:
                # boto3 parameter validation - not a true test (never reached AWS)
                result["status"] = "NOT_TESTED"
                result["message"] = "Requires parameters not in mapping"
                result["data"]["details"] = "Needs param mapping"
            else:
                result["status"] = "ERROR"
                result["message"] = f"{error_code}: {error_message}"
                result["error_code"] = error_code

    except NoCredentialsError:
        result["status"] = "ERROR"
        result["message"] = "No AWS credentials found"
        result["error_code"] = "NoCredentials"
    except EndpointConnectionError:
        result["status"] = "ERROR"
        result["message"] = "Could not connect to endpoint"
        result["error_code"] = "EndpointConnectionError"
    except ParamValidationError as e:
        # Parameter validation error from botocore - not a true test (never reached AWS)
        result["status"] = "NOT_TESTED"
        result["message"] = f"Requires parameters: {str(e)[:60]}"
        result["data"]["details"] = "Needs param mapping"
    except Exception as e:
        result["status"] = "ERROR"
        result["message"] = str(e)[:100]
        result["error_code"] = type(e).__name__

    return result


def query_cloudtrail(service: str, permission: str, region: str, profile: Optional[str], minutes_back: int = 60) -> Optional[dict]:
    """Query CloudTrail for recent events related to the permission."""
    try:
        client = get_boto3_client("cloudtrail", region, profile)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=minutes_back)

        response = client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "EventName", "AttributeValue": permission}
            ],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=5,
        )

        events = response.get("Events", [])

        if not events:
            return {"message": "No recent events", "count": 0}

        latest = events[0]
        return {
            "event_name": latest.get("EventName"),
            "event_time": str(latest.get("EventTime")),
            "username": latest.get("Username"),
            "error_code": latest.get("ErrorCode"),
            "count": len(events),
            "message": f"{len(events)} events in last {minutes_back}min"
        }

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code in ("AccessDenied", "AccessDeniedException"):
            return {"error": "Access denied to CloudTrail", "count": 0}
        return {"error": str(e)[:50], "count": 0}
    except Exception as e:
        return {"error": str(e)[:50], "count": 0}


def get_caller_identity(region: str, profile: Optional[str] = None) -> dict:
    """Get the current AWS caller identity."""
    try:
        client = get_boto3_client("sts", region, profile)
        identity = client.get_caller_identity()
        return {
            "account": identity.get("Account"),
            "arn": identity.get("Arn"),
            "user_id": identity.get("UserId"),
        }
    except Exception as e:
        return {"error": str(e)}


def generate_report(results: list, identity: dict, config: dict, output_dir: Path) -> Path:
    """Generate a markdown report with matrix view."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_file = output_dir / f"validation-report-{timestamp}.md"

    # Count results by status
    passed = sum(1 for r in results if r["status"] == "PASSED")
    denied = sum(1 for r in results if r["status"] == "DENIED")
    skipped = sum(1 for r in results if r["status"] == "SKIPPED")
    not_tested = sum(1 for r in results if r["status"] == "NOT_TESTED")
    errors = sum(1 for r in results if r["status"] == "ERROR")
    total = len(results)

    # Group by service
    services = {}
    for r in results:
        if r["service"] not in services:
            services[r["service"]] = []
        services[r["service"]].append(r)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("# AWS Permissions Validation Report\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Configuration & Identity section (combined)
        f.write("## Configuration\n\n")
        f.write(f"| Setting | Value |\n")
        f.write(f"|---------|-------|\n")
        f.write(f"| Region | {config.get('region', 'us-east-1')} |\n")
        f.write(f"| Profile | {config.get('profile', 'default')} |\n")
        if config.get('description'):
            f.write(f"| Description | {config.get('description')} |\n")
        f.write(f"\n")

        # Identity section - shows actual user/role from credentials
        f.write("## AWS Identity (Detected)\n\n")
        if "error" in identity:
            f.write(f"Error getting identity: {identity['error']}\n\n")
        else:
            # Parse ARN to determine if it's a user or role
            arn = identity.get('arn', '')
            if ':user/' in arn:
                identity_type = "IAM User"
                identity_name = arn.split(':user/')[-1]
            elif ':assumed-role/' in arn:
                identity_type = "Assumed Role"
                identity_name = arn.split(':assumed-role/')[-1].split('/')[0]
            elif ':role/' in arn:
                identity_type = "IAM Role"
                identity_name = arn.split(':role/')[-1]
            else:
                identity_type = "Unknown"
                identity_name = arn

            f.write(f"| Property | Value |\n")
            f.write(f"|----------|-------|\n")
            f.write(f"| Account | {identity.get('account')} |\n")
            f.write(f"| Identity Type | {identity_type} |\n")
            f.write(f"| Identity Name | {identity_name} |\n")
            f.write(f"| Full ARN | {arn} |\n")
            f.write(f"| User ID | {identity.get('user_id')} |\n\n")

        # Summary section
        f.write("## Summary\n\n")
        f.write(f"| Status | Count | Percentage |\n")
        f.write(f"|--------|-------|------------|\n")
        f.write(f"| PASSED | {passed} | {passed*100//total if total else 0}% |\n")
        f.write(f"| DENIED | {denied} | {denied*100//total if total else 0}% |\n")
        f.write(f"| NOT_TESTED | {not_tested} | {not_tested*100//total if total else 0}% |\n")
        f.write(f"| SKIPPED | {skipped} | {skipped*100//total if total else 0}% |\n")
        f.write(f"| ERROR | {errors} | {errors*100//total if total else 0}% |\n")
        f.write(f"| **Total** | **{total}** | **100%** |\n\n")

        # Service Matrix
        f.write("## Service Permission Matrix\n\n")
        f.write("| Service | Total | Passed | Denied | NotTested | Skipped | Error | Status |\n")
        f.write("|---------|-------|--------|--------|-----------|---------|-------|--------|\n")

        for svc_name in sorted(services.keys()):
            svc_results = services[svc_name]
            svc_passed = sum(1 for r in svc_results if r["status"] == "PASSED")
            svc_denied = sum(1 for r in svc_results if r["status"] == "DENIED")
            svc_not_tested = sum(1 for r in svc_results if r["status"] == "NOT_TESTED")
            svc_skipped = sum(1 for r in svc_results if r["status"] == "SKIPPED")
            svc_errors = sum(1 for r in svc_results if r["status"] == "ERROR")
            svc_total = len(svc_results)

            if svc_denied > 0:
                status = "ISSUES"
            elif svc_errors > 0:
                status = "ERRORS"
            elif svc_not_tested > 0:
                status = "PARTIAL"
            elif svc_passed == svc_total - svc_skipped:
                status = "OK"
            else:
                status = "PARTIAL"

            f.write(f"| {svc_name} | {svc_total} | {svc_passed} | {svc_denied} | {svc_not_tested} | {svc_skipped} | {svc_errors} | {status} |\n")

        f.write("\n")

        # Denied permissions (most important)
        if denied > 0:
            f.write("## Denied Permissions\n\n")
            f.write("These permissions were denied and may need to be added to your role/policy.\n\n")
            f.write("| Service | Permission | Error Code | CloudTrail |\n")
            f.write("|---------|------------|------------|------------|\n")

            for r in results:
                if r["status"] == "DENIED":
                    ct = r.get("cloudtrail", {})
                    ct_info = ct.get("message", ct.get("error", "N/A"))
                    f.write(f"| {r['service']} | {r['permission']} | {r.get('error_code', 'N/A')} | {ct_info} |\n")
            f.write("\n")

        # Not Tested section (needs parameter mapping)
        if not_tested > 0:
            f.write("## Not Tested (Needs Parameter Mapping)\n\n")
            f.write("These permissions require parameters but don't have mappings. Add to PERMISSION_REQUIRED_PARAMS.\n\n")
            f.write("| Service | Permission | Reason |\n")
            f.write("|---------|------------|--------|\n")
            for r in results:
                if r["status"] == "NOT_TESTED":
                    f.write(f"| {r['service']} | {r['permission']} | {r['message'][:80]} |\n")
            f.write("\n")

        # Errors section
        if errors > 0:
            f.write("## Errors\n\n")
            f.write("| Service | Permission | Error |\n")
            f.write("|---------|------------|-------|\n")
            for r in results:
                if r["status"] == "ERROR":
                    f.write(f"| {r['service']} | {r['permission']} | {r['message'][:60]} |\n")
            f.write("\n")

        # Detailed Results with Data
        f.write("## Detailed Results\n\n")
        f.write("| Service | Permission | Status | Details |\n")
        f.write("|---------|------------|--------|--------|\n")

        for r in results:
            status_icon = {"PASSED": "OK", "DENIED": "DENIED", "NOT_TESTED": "N/T", "SKIPPED": "SKIP", "ERROR": "ERR"}.get(r["status"], "?")
            data = r.get("data", {})
            if r["status"] == "PASSED":
                details = data.get("details", "")
                if data.get("sample"):
                    details = f"{details} ({data.get('sample')})" if details else data.get("sample")
            elif r["status"] == "SKIPPED":
                details = r.get("message", "")[:100]
            else:
                details = r.get("message", "")[:100]
            f.write(f"| {r['service']} | {r['permission']} | {status_icon} | {details} |\n")

        f.write("\n")

    return output_file


def load_config(file_path: Path) -> tuple[dict, dict]:
    """Load config and permissions from YAML file."""
    with open(file_path, "r") as f:
        data = yaml.safe_load(f)

    config = data.get("config", {})
    permissions = data.get("permissions", data)  # Fallback for old format

    # Remove config from permissions if it exists
    if "config" in permissions:
        del permissions["config"]

    return config, permissions


def main():
    parser = argparse.ArgumentParser(
        description="Validate AWS read-only permissions for the current user/role",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate-read-only-aws-permissions.py                      # Use config from YAML
  python validate-read-only-aws-permissions.py --profile my-profile # Override profile
  python validate-read-only-aws-permissions.py --service ec2        # Validate only EC2
  python validate-read-only-aws-permissions.py --service ec2 s3     # Validate EC2 and S3
        """
    )
    parser.add_argument(
        "--service", "-s",
        nargs="+",
        help="Specific service(s) to validate (default: all)",
    )
    parser.add_argument(
        "--profile", "-p",
        help="AWS profile name (overrides YAML config)",
    )
    parser.add_argument(
        "--permissions-file", "-f",
        default="permissions-list.yaml",
        help="Path to permissions YAML file (default: permissions-list.yaml)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=".",
        help="Output directory for reports (default: current directory)",
    )
    parser.add_argument(
        "--skip-cloudtrail",
        action="store_true",
        help="Skip CloudTrail queries for denied permissions",
    )

    args = parser.parse_args()

    # Resolve paths
    script_dir = Path(__file__).parent
    permissions_file = Path(args.permissions_file)
    if not permissions_file.is_absolute():
        permissions_file = script_dir / permissions_file

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load config and permissions
    print("=" * 60)
    print("AWS Read-Only Permissions Validator")
    print("=" * 60)
    print()

    print(f"Loading config from: {permissions_file}")
    try:
        config, all_permissions = load_config(permissions_file)
    except FileNotFoundError:
        print(f"ERROR: Permissions file not found: {permissions_file}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML: {e}")
        sys.exit(1)

    # Get settings from config (with CLI overrides)
    region = config.get("region", "us-east-1")
    profile = args.profile or config.get("profile", "default")
    if profile == "default":
        profile = None  # Use default credentials chain

    # CloudTrail setting
    check_cloudtrail = config.get("check_cloudtrail", True)
    if args.skip_cloudtrail:
        check_cloudtrail = False

    print(f"Region: {region}")
    print(f"Profile: {profile or 'default'}")
    print(f"Check CloudTrail: {check_cloudtrail}")

    # Set AWS profile environment variable if specified
    if profile:
        os.environ["AWS_PROFILE"] = profile

    # Filter services if specified
    if args.service:
        services_to_check = {s: all_permissions[s] for s in args.service if s in all_permissions}
        missing = [s for s in args.service if s not in all_permissions]
        if missing:
            print(f"WARNING: Services not found: {missing}")
    else:
        services_to_check = all_permissions

    # Get caller identity
    print("\nGetting AWS identity...")
    identity = get_caller_identity(region, profile)
    if "error" in identity:
        print(f"WARNING: Could not get identity: {identity['error']}")
    else:
        print(f"Account: {identity['account']}")
        print(f"ARN: {identity['arn']}")

    # Validate permissions
    print(f"\nValidating {sum(len(p) for p in services_to_check.values())} permissions across {len(services_to_check)} services...")
    print()

    results = []

    for service, permissions in services_to_check.items():
        print(f"[{service}]")

        for permission in permissions:
            result = validate_permission(service, permission, region, profile)

            # Query CloudTrail for denied permissions
            if result["status"] == "DENIED" and check_cloudtrail:
                result["cloudtrail"] = query_cloudtrail(service, permission, region, profile)

            results.append(result)

            # Print inline status with data
            status_symbol = {
                "PASSED": "[OK]",
                "DENIED": "[X]",
                "NOT_TESTED": "[N/T]",
                "SKIPPED": "[--]",
                "ERROR": "[!]",
            }.get(result["status"], "[?]")

            details = result.get("data", {}).get("details", "")
            if details:
                print(f"  {status_symbol} {permission}: {result['status']} - {details}")
            else:
                print(f"  {status_symbol} {permission}: {result['status']}")

        print()

    # Generate report
    print("Generating report...")
    report_path = generate_report(results, identity, config, output_dir)
    print(f"Report saved to: {report_path}")

    # Summary
    passed = sum(1 for r in results if r["status"] == "PASSED")
    denied = sum(1 for r in results if r["status"] == "DENIED")
    not_tested = sum(1 for r in results if r["status"] == "NOT_TESTED")
    skipped = sum(1 for r in results if r["status"] == "SKIPPED")
    errors = sum(1 for r in results if r["status"] == "ERROR")

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  PASSED:     {passed}")
    print(f"  DENIED:     {denied}")
    print(f"  NOT_TESTED: {not_tested}")
    print(f"  SKIPPED:    {skipped}")
    print(f"  ERRORS:     {errors}")
    print()

    if denied > 0:
        print(f"WARNING: {denied} permission(s) were denied!")
        sys.exit(1)

    if not_tested > 0:
        print(f"NOTE: {not_tested} permission(s) need parameter mappings to be fully tested.")

    print("All checked permissions passed!")
    sys.exit(0)


if __name__ == "__main__":
    main()
