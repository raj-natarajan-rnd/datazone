# EventBridge POC - Cross-Account Event-Driven Architecture

This POC demonstrates cross-account event-driven data processing using AWS EventBridge, Lambda, and Step Functions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              WingSafe Account (184838390535)                           │
│                                                                                         │
│                    ┌─────────────────────────────────────────────────┐                    │
│                    │        EventBridge Cross-Account Bus        │                    │
│                    │     aero-platform-cross-account-events      │                    │
│                    └─────────────────┬───────────────────────────┘                    │
└──────────────────────────────────────┼─────────────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  │                  ▼
┌─────────────────────────┐            │            ┌─────────────────────────┐
│  AeroInsight Account    │            │            │  FlightRadar Account    │
│    (707843606641)       │            │            │    (157809907894)       │
│                         │            │            │                         │
│  ┌─────────────────┐    │            │            │  ┌─────────────────┐    │
│  │ Lambda Function │────┼────────────┼────────────┼──│ EventBridge Rule│    │
│  │ (Data Writer)   │    │            │            │  │                 │    │
│  └─────────────────┘    │            │            │  └─────────┬───────┘    │
│           │              │            │            │            │            │
│           ▼              │            │            │            ▼            │
│  ┌─────────────────┐    │            │            │  ┌─────────────────┐    │
│  │ Assumes         │    │            │            │  │ Step Functions  │    │
│  │ DataScientist   │    │            │            │  │ State Machine   │    │
│  │ Role            │    │            │            │  └─────────┬───────┘    │
│  └─────────────────┘    │            │            │            │            │
│           │              │            │            │            ▼            │
│           ▼              │            │            │  ┌─────────────────┐    │
│  ┌─────────────────┐    │            │            │  │ Export & Email  │    │
│  │ Writes to       │    │            │            │  │ Notification    │    │
│  │ Iceberg Table   │    │            │            │  └─────────────────┘    │
│  └─────────────────┘    │            │            │                         │
└─────────────────────────┘            │            └─────────────────────────┘
           │                           │                           ▲
           ▼                           │                           │
┌─────────────────────────┐            │                           │
│  DataLounge Account     │            │                           │
│    (073118366505)       │            │                           │
│                         │            │                           │
│  ┌─────────────────┐    │            │                           │
│  │ Iceberg Tables  │◄───┼────────────┘                           │
│  │ (S3 Storage)    │    │            Data Export                 │
│  └─────────────────┘    │                                        │
└─────────────────────────┘                                        │
                                                                   │
                          Email Notification ─────────────────────┘
```

## Components

### 1. WingSafe Account (184838390535)
- **EventBridge Custom Bus**: `aero-platform-cross-account-events-dev`
- **Cross-Account Policy**: Allows AeroInsight and FlightRadar to publish/consume events
- **Dead Letter Queue**: Handles failed events

### 2. AeroInsight Account (707843606641)
- **Lambda Function**: `aero-data-writer-dev`
  - Assumes DataScientist role for Athena access
  - Writes sample data to `aeronav_db.navigation_waypoints`
  - Publishes events to cross-account EventBridge bus

### 3. FlightRadar Account (157809907894)
- **EventBridge Rule**: Listens for `Data Table Updated` events
- **Step Functions**: Orchestrates data export workflow
- **Lambda Functions**: Export data and send notifications
- **S3 Bucket**: Stores exported data
- **SNS Topic**: Sends email notifications

### 4. DataLounge Account (073118366505)
- **Iceberg Tables**: Stored in S3 buckets
- **Data Storage**: Target for AeroInsight writes, source for FlightRadar exports

## Deployment Steps

### Prerequisites
- AWS Console access to all accounts
- Existing Iceberg table `aeronav_db.navigation_waypoints`
- Updated WingSafe infrastructure with DataScientist role permissions

### Step 1: Deploy EventBridge Infrastructure (WingSafe Account)
```bash
# Deploy using CloudFormation console
Template: wingsafe-eventbridge-infrastructure.yaml
Stack Name: wingsafe-eventbridge-poc-dev
Parameters: Environment=dev
```

### Step 1b: Update WingSafe Main Infrastructure (WingSafe Account)
```bash
# Update existing WingSafe infrastructure stack
Template: WingSafe/cloudformation/wingsafe-main-infrastructure.yaml
Stack Name: wingsafe-infrastructure-dev (existing stack)
Parameters: Environment=dev
```
**Note**: This updates the DataScientist role with EventBridge POC permissions

### Step 2: Deploy Lambda Data Writer (AeroInsight Account)
```bash
# Deploy using CloudFormation console
Template: aeroinsight-lambda-data-writer.yaml
Stack Name: aeroinsight-lambda-poc-dev
Parameters: Environment=dev
```

### Step 3: Deploy Step Functions Consumer (FlightRadar Account)
```bash
# Deploy using CloudFormation console
Template: flightradar-stepfunctions-consumer.yaml
Stack Name: flightradar-stepfunctions-poc-dev
Parameters: 
  Environment=dev
  NotificationEmail=your-email@domain.com
```

### Step 4: Confirm SNS Subscription
- Check email for SNS subscription confirmation
- Click confirmation link

## Testing the POC

### Step-by-Step Testing

#### Step 1: Test EventBridge Infrastructure
```bash
# Verify EventBridge bus is deployed in WingSafe account
# Check CloudFormation stack: wingsafe-eventbridge-poc-dev
```

#### Step 2: Test AeroInsight Lambda Function
```bash
# Test Lambda function independently
python aeroinsight-test-lambda-only.py
```

#### Step 3: Test Complete End-to-End Flow
```bash
# Test complete EventBridge flow (after all components deployed)
python aeroinsight-test-eventbridge-poc.py
```

### Expected Flow
1. **Test script assumes DataScientist role**
2. **Invokes Lambda function in AeroInsight account**
3. **Lambda assumes DataScientist role internally**
4. **Lambda writes 3 sample waypoints to Iceberg table**
5. **Lambda publishes event to EventBridge**
6. **FlightRadar EventBridge rule triggers**
7. **Step Functions workflow starts**
8. **Data export Lambda queries and exports new records**
9. **Notification Lambda sends email**

### Verification

#### After Step 2 Test:
- **CloudWatch Logs**: Check Lambda execution logs in AeroInsight account
- **Athena Console**: Verify query execution in WingSafe workgroup
- **Iceberg Table**: Query `aeronav_db.navigation_waypoints` for POC records
- **EventBridge Metrics**: Check event publishing in WingSafe account

#### After Complete Flow Test:
- **Step Functions Console**: Check execution in FlightRadar account
- **S3 Bucket**: Verify exported CSV in `flightradar-data-export-dev-157809907894`
- **Email**: Receive completion notification
- **CloudWatch Logs**: Monitor all Lambda executions

## Event Schema

```json
{
  "version": "0",
  "id": "unique-event-id",
  "detail-type": "Data Table Updated",
  "source": "aero.aeroinsight",
  "account": "707843606641",
  "time": "2024-01-01T12:00:00Z",
  "region": "us-east-1",
  "detail": {
    "database": "aeronav_db",
    "table": "navigation_waypoints",
    "operation": "INSERT",
    "recordCount": 3,
    "queryExecutionId": "query-id",
    "timestamp": "2024-01-01T12:00:00.000Z",
    "sourceAccount": "707843606641"
  }
}
```

## Key Features

- **Cross-Account Event Bus**: Centralized event hub in WingSafe account
- **Role-Based Security**: Uses existing DataScientist role pattern
- **Iceberg Integration**: Works with existing table structure
- **Step Functions Orchestration**: Reliable workflow execution
- **Email Notifications**: Real-time completion alerts
- **Error Handling**: Retry logic and dead letter queues

## Troubleshooting

### Common Issues
1. **Role Permission Errors**: Ensure DataScientist role has required permissions
2. **EventBridge Access**: Verify cross-account resource policies
3. **Iceberg Table Access**: Confirm table exists and is accessible
4. **Email Not Received**: Check SNS subscription confirmation

### Monitoring
- **CloudWatch Logs**: All Lambda function logs
- **Step Functions Console**: Workflow execution status
- **EventBridge Console**: Event metrics and rules
- **S3 Console**: Exported data files