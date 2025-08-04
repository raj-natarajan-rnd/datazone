- 184838390535
- WingSafe-AeroNav-CrossAccount-dev
- WingSafe-AeroTraffic-CrossAccount-dev
- WingSafe-AeroWeather-CrossAccount-dev
- WingSafe-DataScientist-CrossAccount-dev
- WingSafe-FlightRadarViewer-CrossAccount-dev

# Important
* Redshift to LakeFormation is currently using role chain (2 roles) to access data and permission
* Redshift - no role chain setup yet to demo restricte columns - schema has been created in Redshift for restricted columns 
* Redshift - column level security thru LakeFormation is super complex and requires schema for different roles (if sole is used for restricted columns). Need to spend more time.
* SSO user -> Permission Set for Application / user level -> Login to Analytics account to use Tools -> May have 1 or more application level permission set -> Select permission set -> switch to local role specific to that permission set. This is a good method and works fine with console and python. SSO may be able to automatically assume another role - but needs deep dive


-
-
-
-
-
-
-
-
-
-
-
-
-
-
-
-
-


















# Aero Data Platform - Multi-Account Architecture

A comprehensive data platform demonstrating cross-account data governance, column-level security, and role-based access control using AWS services including S3, Glue, LakeFormation, Athena, and Redshift Serverless.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    BaseCamp     │    │   AeroInsight   │    │    WingSafe     │    │   DataLounge    │    │   FlightRadar   │
│  (729272315322) │    │  (707843606641) │    │  (184838390535) │    │  (073118366505) │    │  (157809907894) │
│                 │    │                 │    │                 │    │                 │    │                 │
│ • SSO Identity  │    │ • Analytics Hub │    │ • Data Catalog  │    │ • 3 Aero Apps   │    │ • Radar Data    │
│ • Permission    │◄──►│ • Athena        │◄──►│ • LakeFormation │◄──►│ • 6 Tables      │    │ • 1 Table       │
│   Sets (5)      │    │ • Redshift      │    │ • 5 Cross-Roles │    │ • S3 Storage    │    │ • S3 Storage    │
│ • User Access   │    │ • Demo Scripts  │    │ • Column Security│    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Applications & Data

### FlightRadar Application
- **Database**: `flightradar_db`
- **Table**: `radar_detections` (5 rows)
- **Restricted Columns**: `speed_knots`, `heading_degrees`
- **Use Case**: Flight tracking and radar detection

### DataLounge Applications (3 Applications)

#### 1. AeroNav (Navigation)
- **Database**: `aeronav_db`
- **Tables**: 
  - `navigation_waypoints` (10 rows) - Navigation waypoints with coordinates
  - `flight_routes` (10 rows) - Flight routes with fuel consumption
- **Restricted Columns**: `frequency_mhz`, `magnetic_variation`, `fuel_consumption_gallons`, `altitude_profile`

#### 2. AeroWeather (Weather)
- **Database**: `aeroweather_db`
- **Tables**:
  - `weather_observations` (10 rows) - Current weather data
  - `weather_forecasts` (10 rows) - Weather predictions
- **Restricted Columns**: `barometric_pressure_hpa`, `wind_speed_knots`, `wind_direction_degrees`, `predicted_wind_speed_knots`, `predicted_wind_direction_degrees`, `forecast_confidence`

#### 3. AeroTraffic (Air Traffic Control)
- **Database**: `aerotraffic_db`
- **Tables**:
  - `air_traffic_control` (10 rows) - ATC sector information
  - `runway_operations` (10 rows) - Runway operations data
- **Restricted Columns**: `frequency_mhz`, `coordination_required`, `emergency_status`, `fuel_consumed_gallons`, `taxi_time_minutes`

## Account Resources

### BaseCamp Account (729272315322)
**Purpose**: SSO Identity Center and Permission Management

| Resource Type | File | Description |
|---|---|---|
| CloudFormation | `basecamp-aeroinsight-permission-sets.yaml` | DataScientist and FlightRadar viewer permission sets |
| CloudFormation | `basecamp-datalounge-permission-sets.yaml` | 3 application-specific permission sets (AeroNav, AeroWeather, AeroTraffic) |

**Total Permission Sets**: 5
- `AeroInsight-DataScientist-Admin` (full access)
- `AeroInsight-FlightRadar-Viewer` (restricted)
- `AeroInsight-AeroNav-Viewer` (restricted)
- `AeroInsight-AeroWeather-Viewer` (restricted)
- `AeroInsight-AeroTraffic-Viewer` (restricted)

### AeroInsight Account (707843606641)
**Purpose**: Analytics and Query Hub

| Resource Type | File | Description |
|---|---|---|
| CloudFormation | `aeroinsight-main-infrastructure.yaml` | Athena workgroup and results bucket |
| CloudFormation | `aeroinsight-federated-catalog.yaml` | Cross-account catalog access |
| CloudFormation | `aeroinsight-redshift-serverless.yaml` | Redshift Serverless for analytics |
| CloudFormation | `aeroinsight-quicksight.yaml` | QuickSight dashboards |
| Python | `setup-cross-account-catalog.py` | Configure federated catalog access |
| Python | `setup-redshift-external-schema.py` | Setup external schemas |
| Python | `demo-datalounge-multi-application.py` | Demo all 4 DataLounge applications |
| Python | `demo-flightradar-application.py` | Demo FlightRadar application |
| Python | `verify-lakeformation-permissions.py` | Test column-level security |

**Resources Deployed**:
- Athena workgroup and results S3 bucket
- Redshift Serverless namespace and workgroup
- External data catalogs for cross-account access
- QuickSight analysis and dashboards

### WingSafe Account (184838390535)
**Purpose**: Centralized Data Catalog and Governance

| Resource Type | File | Description |
|---|---|---|
| CloudFormation | `wingsafe-main-infrastructure.yaml` | Core infrastructure, FlightRadar database |
| CloudFormation | `wingsafe-datalounge-infrastructure.yaml` | DataLounge databases and cross-account roles |
| Python | `create-table-and-insert-data.py` | Create FlightRadar table and data |
| Python | `create-datalounge-tables-and-data.py` | Create all DataLounge tables and data |
| Python | `setup-lakeformation-permissions.py` | FlightRadar column-level permissions |
| Python | `setup-datalounge-lakeformation-permissions.py` | DataLounge column-level permissions |

**Resources Deployed**:
- 4 Glue databases (`flightradar_db`, `aeronav_db`, `aeroweather_db`, `aerotraffic_db`)
- 7 Iceberg tables (1 FlightRadar + 6 DataLounge)
- 5 cross-account IAM roles with LakeFormation permissions
- Athena workgroup for centralized queries

### DataLounge Account (073118366505)
**Purpose**: Multi-Application Data Storage

| Resource Type | File | Description |
|---|---|---|
| CloudFormation | `datalounge-main-infrastructure.yaml` | S3 buckets for 3 applications with cross-account policies |

**Resources Deployed**:
- 3 S3 buckets for Iceberg data storage
- 1 S3 bucket for Athena query results
- Cross-account bucket policies for WingSafe access
- Athena workgroup for local operations

### FlightRadar Account (157809907894)
**Purpose**: Flight Radar Data Storage

| Resource Type | File | Description |
|---|---|---|
| CloudFormation | `flightradar-main-infrastructure.yaml` | S3 bucket and Athena workgroup |

**Resources Deployed**:
- 1 S3 bucket for Iceberg data storage
- 1 S3 bucket for Athena query results
- Athena workgroup for data capture

## Deployment Steps

### Prerequisites
- AWS CLI configured with appropriate profiles for each account
- SSO Instance ID: `ssoins-7223459730eac9ac`

### Step 1: Deploy BaseCamp Permission Sets
```bash
# Deploy FlightRadar permission sets
aws cloudformation deploy \
  --template-file BaseCamp/cloudformation/basecamp-aeroinsight-permission-sets.yaml \
  --stack-name basecamp-aeroinsight-permission-sets-dev \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides SSOInstanceId=ssoins-7223459730eac9ac \
  --profile basecamp-dev

# Deploy DataLounge permission sets
aws cloudformation deploy \
  --template-file BaseCamp/cloudformation/basecamp-datalounge-permission-sets.yaml \
  --stack-name basecamp-datalounge-permission-sets-dev \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides SSOInstanceId=ssoins-7223459730eac9ac \
  --profile basecamp-dev
```

### Step 2: Deploy Infrastructure
```bash
# Deploy FlightRadar infrastructure
aws cloudformation deploy \
  --template-file FlightRadar/cloudformation/flightradar-main-infrastructure.yaml \
  --stack-name flightradar-infrastructure-dev \
  --region us-east-1 \
  --profile flightradar-dev

# Deploy DataLounge infrastructure
aws cloudformation deploy \
  --template-file DataLounge/cloudformation/datalounge-main-infrastructure.yaml \
  --stack-name datalounge-infrastructure-dev \
  --region us-east-1 \
  --profile datalounge-dev

# Deploy WingSafe core infrastructure
aws cloudformation deploy \
  --template-file WingSafe/cloudformation/wingsafe-main-infrastructure.yaml \
  --stack-name wingsafe-infrastructure-dev \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile wingsafe-dev

# Deploy WingSafe DataLounge infrastructure
aws cloudformation deploy \
  --template-file WingSafe/cloudformation/wingsafe-datalounge-infrastructure.yaml \
  --stack-name wingsafe-datalounge-infrastructure-dev \
  --region us-east-1 \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile wingsafe-dev

# Deploy AeroInsight infrastructure
aws cloudformation deploy \
  --template-file AeroInsight/cloudformation/aeroinsight-main-infrastructure.yaml \
  --stack-name aeroinsight-infrastructure-dev \
  --region us-east-1 \
  --profile aeroinsight-dev

aws cloudformation deploy \
  --template-file AeroInsight/cloudformation/aeroinsight-redshift-serverless.yaml \
  --stack-name aeroinsight-redshift-dev \
  --region us-east-1 \
  --profile aeroinsight-dev
```

### Step 3: Create Tables and Data
```bash
# Create FlightRadar table and data (from WingSafe)
cd WingSafe/python
python create-table-and-insert-data.py

# Create DataLounge tables and data (from WingSafe)
python create-datalounge-tables-and-data.py
```

### Step 4: Setup Cross-Account Access
```bash
# Setup AeroInsight cross-account catalog (from AeroInsight)
cd AeroInsight/python
python setup-cross-account-catalog.py
python setup-redshift-external-schema.py
```

### Step 5: Configure LakeFormation Permissions
```bash
# Setup FlightRadar permissions (from WingSafe)
cd WingSafe/python
python setup-lakeformation-permissions.py

# Setup DataLounge permissions (from WingSafe)
python setup-datalounge-lakeformation-permissions.py
```

## Test Scripts

| Script | Account | Purpose | What It Tests |
|---|---|---|---|
| `verify-lakeformation-permissions.py` | AeroInsight | LakeFormation Security | Tests all 5 roles across 4 databases, verifies column-level restrictions |
| `verify-datalounge-setup.py` | AeroInsight | DataLounge Setup | Verifies Glue catalog registration and table creation |
| `demo-flightradar-application.py` | AeroInsight | FlightRadar Demo | Demonstrates DataScientist vs FlightRadarViewer access with column restrictions |
| `demo-datalounge-multi-application.py` | AeroInsight | DataLounge Demo | Demonstrates all 4 roles (DataScientist + 3 app-specific) with column restrictions |
| `demo-redshift-column-level-security.py` | AeroInsight | Redshift Security | Tests column-level security in Redshift Serverless |

## Security Model

### Role-Based Access Control

| Role | Permission Set | Database Access | Column Restrictions | Use Case |
|---|---|---|---|---|
| DataScientist | `AeroInsight-DataScientist-Admin` | All databases, all columns | None | Full analytics, research, ML |
| FlightRadarViewer | `AeroInsight-FlightRadar-Viewer` | `flightradar_db` only | `speed_knots`, `heading_degrees` | Basic flight tracking |
| AeroNav | `AeroInsight-AeroNav-Viewer` | `aeronav_db` only | Navigation frequencies, fuel data | Route planning |
| AeroWeather | `AeroInsight-AeroWeather-Viewer` | `aeroweather_db` only | Detailed weather measurements | Basic weather queries |
| AeroTraffic | `AeroInsight-AeroTraffic-Viewer` | `aerotraffic_db` only | ATC frequencies, emergency status | Traffic monitoring |

### Key Security Features
- **Cross-Account Architecture**: Data isolation with centralized governance
- **Column-Level Security**: LakeFormation controls column visibility by role
- **Application Isolation**: Each application has dedicated database and restricted access
- **Centralized Catalog**: All metadata managed in WingSafe with distributed data storage
- **Audit Trail**: Complete access logging through CloudTrail and LakeFormation

## Data Flow

1. **Data Storage**: Raw data stored in application-specific S3 buckets (DataLounge/FlightRadar)
2. **Catalog Registration**: Tables registered in WingSafe centralized Glue catalog
3. **Permission Management**: LakeFormation controls column-level access by role
4. **User Access**: Users authenticate via SSO, assume appropriate permission sets
5. **Query Execution**: Athena/Redshift queries data with role-based column filtering
6. **Analytics**: Results delivered based on user's permission level

This architecture demonstrates enterprise-grade data governance with fine-grained access control across multiple AWS accounts and applications.
