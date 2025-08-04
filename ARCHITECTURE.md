# Aero Data Platform - Detailed Architecture

## High-Level Multi-Account Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           AERO DATA PLATFORM - MULTI-ACCOUNT ARCHITECTURE                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    BaseCamp     │    │   AeroInsight   │    │    WingSafe     │    │   DataLounge    │    │   FlightRadar   │
│  (729272315322) │    │  (707843606641) │    │  (184838390535) │    │  (073118366505) │    │  (157809907894) │
│                 │    │                 │    │                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ SSO Identity│ │    │ │ Analytics   │ │    │ │ Data        │ │    │ │ 3 Aero Apps │ │    │ │ Radar Data  │ │
│ │ Center      │ │◄──►│ │ Hub         │ │◄──►│ │ Catalog     │ │◄──►│ │ Storage     │ │    │ │ Storage     │ │
│ │             │ │    │ │             │ │    │ │             │ │    │ │             │ │    │ │             │ │
│ │ • 5 Perm    │ │    │ │ • Athena    │ │    │ │ • Glue      │ │    │ │ • 3 S3      │ │    │ │ • 1 S3      │ │
│ │   Sets      │ │    │ │ • Redshift  │ │    │ │ • LakeFmt   │ │    │ │   Buckets   │ │    │ │   Bucket    │ │
│ │ • User      │ │    │ │ • QuickSight│ │    │ │ • 4 DBs     │ │    │ │ • Athena    │ │    │ │ • Athena    │ │
│ │   Access    │ │    │ │ • Demo      │ │    │ │ • 7 Tables  │ │    │ │   Workgroup │ │    │ │   Workgroup │ │
│ └─────────────┘ │    │ │   Scripts   │ │    │ │ • 5 Roles   │ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    │ └─────────────┘ │    │ └─────────────┘ │    └─────────────────┘    └─────────────────┘
                       └─────────────────┘    └─────────────────┘
```

## Detailed Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                    DATA FLOW ARCHITECTURE                                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

    USER ACCESS                    ANALYTICS LAYER                   GOVERNANCE LAYER                   DATA STORAGE LAYER
┌─────────────────┐           ┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│   BaseCamp      │           │   AeroInsight   │              │    WingSafe     │              │ DataLounge +    │
│                 │           │                 │              │                 │              │ FlightRadar     │
│ ┌─────────────┐ │   SSO     │ ┌─────────────┐ │  Cross-Acc  │ ┌─────────────┐ │  Cross-Acc  │ ┌─────────────┐ │
│ │DataScientist│ │◄─────────►│ │   Athena    │ │◄───────────►│ │ Glue Catalog│ │◄───────────►│ │ S3 Buckets  │ │
│ │   (Full)    │ │           │ │ Workgroup   │ │             │ │             │ │             │ │             │ │
│ └─────────────┘ │           │ └─────────────┘ │             │ │ • 4 DBs     │ │             │ │ • AeroNav   │ │
│                 │           │                 │             │ │ • 7 Tables  │ │             │ │ • AeroWeath │ │
│ ┌─────────────┐ │           │ ┌─────────────┐ │             │ │ • Metadata  │ │             │ │ • AeroTraf  │ │
│ │FlightRadar  │ │◄─────────►│ │  Redshift   │ │             │ └─────────────┘ │             │ │ • FlightRad │ │
│ │ (Restricted)│ │           │ │ Serverless  │ │             │                 │             │ └─────────────┘ │
│ └─────────────┘ │           │ └─────────────┘ │             │ ┌─────────────┐ │             └─────────────────┘
│                 │           │                 │             │ │LakeFormation│ │
│ ┌─────────────┐ │           │ ┌─────────────┐ │             │ │             │ │
│ │  AeroNav    │ │◄─────────►│ │ QuickSight  │ │             │ │ • Column    │ │
│ │ (Restricted)│ │           │ │ Dashboards  │ │             │ │   Security  │ │
│ └─────────────┘ │           │ └─────────────┘ │             │ │ • Role      │ │
│                 │           │                 │             │ │   Mapping   │ │
│ ┌─────────────┐ │           │ ┌─────────────┐ │             │ │ • Audit     │ │
│ │ AeroWeather │ │◄─────────►│ │   Demo      │ │             │ │   Trail     │ │
│ │ (Restricted)│ │           │ │  Scripts    │ │             │ └─────────────┘ │
│ └─────────────┘ │           │ └─────────────┘ │             └─────────────────┘
│                 │           └─────────────────┘
│ ┌─────────────┐ │
│ │ AeroTraffic │ │
│ │ (Restricted)│ │
│ └─────────────┘ │
└─────────────────┘
```

## Application Data Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              APPLICATION DATA ARCHITECTURE                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

FLIGHTRADAR APPLICATION                                    DATALOUNGE APPLICATIONS (3)
┌─────────────────────────────────┐                      ┌─────────────────────────────────────────────────────────────────────────────┐
│         FlightRadar             │                      │                           DataLounge                                        │
│      (157809907894)             │                      │                        (073118366505)                                      │
│                                 │                      │                                                                             │
│ ┌─────────────────────────────┐ │                      │ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────────────┐ │
│ │        S3 Storage           │ │                      │ │    AeroNav      │ │   AeroWeather   │ │       AeroTraffic           │ │
│ │                             │ │                      │ │   Navigation    │ │     Weather     │ │   Air Traffic Control       │ │
│ │ • flightradar-iceberg-dev   │ │                      │ │                 │ │                 │ │                             │ │
│ │ • Radar detection data      │ │                      │ │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────────────────┐ │ │
│ │ • Iceberg format            │ │                      │ │ │ S3 Storage  │ │ │ │ S3 Storage  │ │ │ │      S3 Storage         │ │ │
│ └─────────────────────────────┘ │                      │ │ │             │ │ │ │             │ │ │ │                         │ │ │
│                                 │                      │ │ │ • aeronav-  │ │ │ │ • aeroweath-│ │ │ │ • aerotraffic-iceberg-  │ │ │
│ ┌─────────────────────────────┐ │                      │ │ │   iceberg-  │ │ │ │   iceberg-  │ │ │ │   dev                   │ │ │
│ │      WingSafe Catalog       │ │                      │ │ │   dev       │ │ │ │   dev       │ │ │ │ • ATC & runway data     │ │ │
│ │                             │ │                      │ │ │ • Waypoints │ │ │ │ • Weather   │ │ │ │ • Iceberg format        │ │ │
│ │ Database: flightradar_db    │ │                      │ │ │ • Routes    │ │ │ │   obs/fcst  │ │ │ └─────────────────────────┘ │ │
│ │ Table: radar_detections     │ │                      │ │ │ • Iceberg   │ │ │ │ • Iceberg   │ │ └─────────────────────────────┘ │
│ │ Rows: 5                     │ │                      │ │ └─────────────┘ │ │ └─────────────┘ │                                 │
│ │                             │ │                      │ └─────────────────┘ └─────────────────┘                                 │
│ │ Restricted Columns:         │ │                      │                                                                             │
│ │ • speed_knots              │ │                      │ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ • heading_degrees          │ │                      │ │                        WingSafe Catalog                                 │ │
│ └─────────────────────────────┘ │                      │ │                                                                         │ │
└─────────────────────────────────┘                      │ │ AeroNav DB:           AeroWeather DB:        AeroTraffic DB:           │ │
                                                          │ │ • navigation_waypts   • weather_obs          • air_traffic_control     │ │
                                                          │ │ • flight_routes       • weather_forecasts    • runway_operations       │ │
                                                          │ │ (10 rows each)        (10 rows each)        (10 rows each)            │ │
                                                          │ │                                                                         │ │
                                                          │ │ Restricted Columns:   Restricted Columns:   Restricted Columns:       │ │
                                                          │ │ • frequency_mhz       • barometric_press    • frequency_mhz            │ │
                                                          │ │ • magnetic_variation  • wind_speed_knots    • coordination_required    │ │
                                                          │ │ • fuel_consumption    • wind_direction       • emergency_status         │ │
                                                          │ │ • altitude_profile    • predicted_wind_*     • fuel_consumed_gallons   │ │
                                                          │ │                       • forecast_confidence • taxi_time_minutes        │ │
                                                          │ └─────────────────────────────────────────────────────────────────────────┘ │
                                                          └─────────────────────────────────────────────────────────────────────────────┘
```

## Security & Permission Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            SECURITY & PERMISSION ARCHITECTURE                                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

SSO PERMISSION SETS (BaseCamp)                           CROSS-ACCOUNT ROLES (WingSafe)                    LAKEFORMATION PERMISSIONS
┌─────────────────────────────────┐                    ┌─────────────────────────────────┐                ┌─────────────────────────────┐
│         BaseCamp                │                    │           WingSafe              │                │         WingSafe            │
│      (729272315322)             │                    │        (184838390535)           │                │      (184838390535)         │
│                                 │                    │                                 │                │                             │
│ ┌─────────────────────────────┐ │                    │ ┌─────────────────────────────┐ │                │ ┌─────────────────────────┐ │
│ │    Permission Sets (5)      │ │                    │ │    Cross-Account Roles (5)  │ │                │ │   Column-Level Security │ │
│ │                             │ │                    │ │                             │ │                │ │                         │ │
│ │ 1. DataScientist-Admin      │ │◄──────────────────►│ │ 1. DataScientist-Role       │ │◄──────────────►│ │ FlightRadar:            │ │
│ │    • Full access to all     │ │                    │ │    • Full catalog access    │ │                │ │ • DENY: speed_knots     │ │
│ │    • No restrictions        │ │                    │ │    • All databases          │ │                │ │ • DENY: heading_degrees │ │
│ │                             │ │                    │ │                             │ │                │ │                         │ │
│ │ 2. FlightRadar-Viewer       │ │◄──────────────────►│ │ 2. FlightRadar-Role         │ │                │ │ AeroNav:                │ │
│ │    • flightradar_db only    │ │                    │ │    • flightradar_db only    │ │                │ │ • DENY: frequency_mhz   │ │
│ │    • Column restrictions    │ │                    │ │    • Column filtering       │ │                │ │ • DENY: magnetic_var    │ │
│ │                             │ │                    │ │                             │ │                │ │ • DENY: fuel_consump    │ │
│ │ 3. AeroNav-Viewer           │ │◄──────────────────►│ │ 3. AeroNav-Role             │ │                │ │ • DENY: altitude_prof   │ │
│ │    • aeronav_db only        │ │                    │ │    • aeronav_db only        │ │                │ │                         │ │
│ │    • Column restrictions    │ │                    │ │    • Column filtering       │ │                │ │ AeroWeather:            │ │
│ │                             │ │                    │ │                             │ │                │ │ • DENY: barometric_*    │ │
│ │ 4. AeroWeather-Viewer       │ │◄──────────────────►│ │ 4. AeroWeather-Role         │ │                │ │ • DENY: wind_speed_*    │ │
│ │    • aeroweather_db only    │ │                    │ │    • aeroweather_db only    │ │                │ │ • DENY: wind_direction  │ │
│ │    • Column restrictions    │ │                    │ │    • Column filtering       │ │                │ │ • DENY: predicted_*     │ │
│ │                             │ │                    │ │                             │ │                │ │ • DENY: forecast_conf   │ │
│ │ 5. AeroTraffic-Viewer       │ │◄──────────────────►│ │ 5. AeroTraffic-Role         │ │                │ │                         │ │
│ │    • aerotraffic_db only    │ │                    │ │    • aerotraffic_db only    │ │                │ │ AeroTraffic:            │ │
│ │    • Column restrictions    │ │                    │ │    • Column filtering       │ │                │ │ • DENY: frequency_mhz   │ │
│ └─────────────────────────────┘ │                    │ └─────────────────────────────┘ │                │ │ • DENY: coordination_*  │ │
└─────────────────────────────────┘                    └─────────────────────────────────┘                │ │ • DENY: emergency_*     │ │
                                                                                                           │ │ • DENY: fuel_consumed   │ │
                                                                                                           │ │ • DENY: taxi_time_*     │ │
                                                                                                           │ └─────────────────────────┘ │
                                                                                                           └─────────────────────────────┘
```

## Query Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                QUERY EXECUTION FLOW                                                                        │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

1. USER AUTHENTICATION                2. ROLE ASSUMPTION                   3. QUERY EXECUTION                    4. DATA ACCESS
┌─────────────────┐                 ┌─────────────────┐                 ┌─────────────────┐                 ┌─────────────────┐
│   User Login    │                 │  Cross-Account  │                 │    Analytics    │                 │  Data Storage   │
│                 │                 │  Role Switch    │                 │    Services     │                 │                 │
│ ┌─────────────┐ │    SSO Auth     │ ┌─────────────┐ │   Assume Role   │ ┌─────────────┐ │   Read Data     │ ┌─────────────┐ │
│ │   BaseCamp  │ │◄───────────────►│ │ AeroInsight │ │◄───────────────►│ │   Athena    │ │◄───────────────►│ │ DataLounge  │ │
│ │             │ │                 │ │             │ │                 │ │             │ │                 │ │             │ │
│ │ • User      │ │                 │ │ • Assume    │ │                 │ │ • Query     │ │                 │ │ • S3 Bucket │ │
│ │   selects   │ │                 │ │   WingSafe  │ │                 │ │   Planning  │ │                 │ │ • Iceberg   │ │
│ │   permission│ │                 │ │   role      │ │                 │ │ • Column    │ │                 │ │   Tables    │ │
│ │   set       │ │                 │ │ • Get temp  │ │                 │ │   Filtering │ │                 │ │             │ │
│ │             │ │                 │ │   creds     │ │                 │ │ • Result    │ │                 │ │             │ │
│ └─────────────┘ │                 │ └─────────────┘ │                 │ │   Return    │ │                 │ └─────────────┘ │
└─────────────────┘                 └─────────────────┘                 │ └─────────────┘ │                 └─────────────────┘
                                                                        │                 │
                                                                        │ ┌─────────────┐ │                 ┌─────────────────┐
                                                                        │ │  Redshift   │ │◄───────────────►│ │ FlightRadar │ │
                                                                        │ │ Serverless  │ │                 │ │             │ │
                                                                        │ │             │ │                 │ │ • S3 Bucket │ │
                                                                        │ │ • External  │ │                 │ │ • Iceberg   │ │
                                                                        │ │   Schema    │ │                 │ │   Tables    │ │
                                                                        │ │ • Column    │ │                 │ │             │ │
                                                                        │ │   Security  │ │                 │ └─────────────┘ │
                                                                        │ └─────────────┘ │                 └─────────────────┘
                                                                        └─────────────────┘

5. GOVERNANCE LAYER (WingSafe)
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                    GOVERNANCE LAYER                                                                        │
│                                                      WingSafe                                                                              │
│                                                  (184838390535)                                                                           │
│                                                                                                                                             │
│ ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────────────────┐ │
│ │  Glue Catalog   │    │ LakeFormation   │    │   IAM Roles     │    │  CloudTrail     │    │           Metadata                      │ │
│ │                 │    │                 │    │                 │    │                 │    │                                         │ │
│ │ • 4 Databases   │    │ • Column-level  │    │ • 5 Cross-acc   │    │ • Access logs   │    │ • Table schemas                         │ │
│ │ • 7 Tables      │    │   permissions   │    │   roles         │    │ • Query audit   │    │ • Partition info                        │ │
│ │ • Schema mgmt   │    │ • Role mapping  │    │ • Temp creds    │    │ • Permission    │    │ • Column definitions                    │ │
│ │ • Partitions    │    │ • Grant/Revoke  │    │ • Policy attach │    │   changes       │    │ • Data location                         │ │
│ └─────────────────┘    │ • Audit trail   │    │ └─────────────────┘    │ └─────────────────┘    │ • Statistics                            │ │
│                        │ └─────────────────┘                                                    │ └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Service | Purpose | Account |
|-------|---------|---------|---------|
| **Identity** | AWS SSO Identity Center | User authentication & authorization | BaseCamp |
| **Analytics** | Amazon Athena | Interactive SQL queries | AeroInsight |
| **Analytics** | Amazon Redshift Serverless | Data warehousing & complex analytics | AeroInsight |
| **Visualization** | Amazon QuickSight | Dashboards & reporting | AeroInsight |
| **Catalog** | AWS Glue Data Catalog | Metadata management | WingSafe |
| **Governance** | AWS LakeFormation | Fine-grained access control | WingSafe |
| **Storage** | Amazon S3 | Data lake storage | DataLounge, FlightRadar |
| **Format** | Apache Iceberg | Table format with ACID transactions | All data accounts |
| **Audit** | AWS CloudTrail | Access logging & compliance | All accounts |
| **Orchestration** | AWS CloudFormation | Infrastructure as Code | All accounts |

## Key Architectural Principles

1. **Separation of Concerns**: Each account has a specific purpose (identity, analytics, governance, storage)
2. **Centralized Governance**: All metadata and permissions managed in WingSafe
3. **Distributed Storage**: Data stored close to applications while maintaining central catalog
4. **Fine-Grained Security**: Column-level access control based on user roles
5. **Cross-Account Architecture**: Secure data sharing without data movement
6. **Audit & Compliance**: Complete access trail through CloudTrail and LakeFormation
7. **Scalable Design**: Serverless services for automatic scaling
8. **Modern Data Formats**: Apache Iceberg for ACID transactions and schema evolution