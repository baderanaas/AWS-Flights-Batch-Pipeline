# AWS Flights Data Pipeline

![flights-aws-batch-pipeline](https://github.com/user-attachments/assets/52b62b5c-4eb3-4a67-b37d-4a63c32934c2)
## Overview
An automated pipeline for collecting, processing, and analyzing flight data from multiple German airports using AWS services. The pipeline runs daily via EventBridge schedule, transforming raw flight data into actionable insights through a series of AWS services including Step Functions, Lambda, Glue, and Redshift.

## Architecture Components

### 1. Data Collection (Step Functions)
- **EventBridge Trigger**: Scheduled daily execution of Step Functions workflow
- **Parallel Lambda Functions**: Simultaneous data collection for airports:
  - Berlin (EDDB)
  - Munich (EDDM) 
  - Hamburg (EDDH)
  - Dusseldorf (EDDL)
  - Frankfurt (EDDF)
- **Initial Storage**: Raw JSON data stored in designated S3 bucket paths

### 2. Data Transformation
- **Lambda Trigger**: S3 event notifications trigger transformation on new JSON files
- **Format Conversion**: JSON to Parquet transformation for optimal querying
- **Organization**: Data structured by airport code and date
- **Cataloging**: Glue crawlers maintain up-to-date data catalog
- **Quality Checks**: Validation of data completeness and format

### 3. Data Processing
- **Glue Jobs**: 
  - Combines data from all airports
  - Performs necessary transformations
  - Handles data deduplication
  - Ensures data quality and consistency
- **Storage**: 
  - Results stored in partitioned format for efficient querying
  - Organized by flight date for optimal performance
- **Metadata**: Final crawler updates AWS Glue Data Catalog

### 4. Data Loading & Analytics
- **Redshift Integration**: 
  - Automated loading into Redshift tables
  - Optimized table design for analytical queries

## Data Flow
1. `Raw JSON` (S3/raw/) → 
2. `Parquet Files` (S3/transformed/) → 
3. `Processed Data` (S3/processed/) → 
4. `Redshift Tables` → 

## Monitoring & Alerts

### Pipeline Monitoring
- **EventBridge Rules**: Monitor state changes of:
  - Step Functions execution
  - Lambda function completion
  - Glue job status
  - Redshift load status

### Notification System
- **SNS Topics**: 
  - Success/failure notifications
  - Job completion alerts
  - Error reporting
- **Email Integration**: Automated alerts for:
  - Pipeline failures
  - Data quality issues
  - Processing delays

### Performance Tracking
- **CloudWatch Metrics**:
  - Lambda execution times
  - Glue job performance
  - Data processing volumes

## Storage Structure
```
flights-data-storage/
├── raw/                    # Raw JSON files
│   ├── EDDB/              # Berlin Airport data
│   ├── EDDM/              # Munich Airport data
│   ├── EDDH/              # Hamburg Airport data
│   ├── EDDL/              # Dusseldorf Airport data
│   └── EDDF/              # Frankfurt Airport data
├── transformed/           # Parquet files by airport
│   ├── EDDB/
│   ├── EDDM/
│   ├── EDDH/
│   ├── EDDL/
│   └── EDDF/
└── processed/            # Final processed data
    └── all_data/         # Combined airport data
        └── flight-date={date}/
```

## Prerequisites

### AWS Services
- Active AWS Account
- Services enabled:
  - Step Functions
  - Lambda
  - S3
  - Glue
  - Redshift Serverless
  - EventBridge
  - SNS

### IAM Requirements
- Roles configured for:
  - Lambda execution
  - Glue job processing
  - Redshift data loading

### Infrastructure
- Redshift Serverless workspace
- Appropriate VPC configuration
- S3 bucket with proper permissions

## Maintenance

### Daily Operations
- Monitor pipeline execution
- Verify data completeness
- Check error notifications
- Review performance metrics

### Regular Tasks
- Update Lambda layers as needed
- Review and optimize Glue jobs
- Maintain Redshift tables

### Troubleshooting
- Check CloudWatch logs for errors
- Verify S3 bucket permissions
- Ensure IAM roles are correct
- Monitor resource utilization
