# Permissions specific to setup Glue Crawler and Athena for schema Changes
## LakeFormation Admin Role
-  LF does not do the schema evolution (changes to iceberg tables). Changes on tables are made using ALTER TABLE thru Athena or Glue Spark engines
-  LF principle needs CREATE / ALTER on new tables but this permission is not used in Crawler.  
## Data Producer Role 
- Create and evolve iceberg tables (DDL), run Glue Spark or Athena for DDL/DML and run the crawler to keep Glue Catalog entries up-to-date.
- **Following permissions are required to run Glue crawler** - this is in addition to all the other S3 / Athena / Glue / LF permissions for this role
### IAM permissions attached to this role to make changes to table:
- Managed: service-role/AWSGlueServiceRole
- lakeformation:GetDataAccess
- Glue Catalog read/DDL used by crawlers: glue:GetDatabase, glue:GetTable*, glue:GetPartitions*, glue:CreateTable, glue:UpdateTable, glue:BatchCreatePartition, glue:BatchUpdatePartition
- S3 read of the crawled prefixes: s3:ListBucket and s3:GetObject*
### LakeFormation Grants
- On database: CREATE_TABLE, DESCRIBE.
- On tables (or wildcard): ALTER, DESCRIBE (and SELECT so the crawler can read via LF).
- On data location (registered S3 bucket/prefix): DATA_LOCATION_ACCESS.
- Enable UseLakeFormationCredentials=true in the crawler config
### Scheduling Permissions
- glue:CreateCrawler, glue:UpdateCrawler, glue:UpdateCrawlerSchedule, glue:GetCrawler, glue:GetCrawlers, glue:StartCrawlerSchedule / glue:StopCrawlerSchedule
- glue:StartCrawler, glue:StopCrawler
- glue:BatchGetCrawlers, glue:GetCrawler, glue:GetCrawlerMetrics, glue:GetCrawlers
- iam:PassRole (on the crawler execution role ARN)
- If crawler is started by a trigger then need glue:*Trigger* permissions and iam:PassRole
### Permissions for Athena
- athena:StartQueryExecution, athena:GetQueryExecution, athena:GetQueryResults and other workgroup related permissions
- s3:PutObject is required to make changes to table and all other regular s3 permissions for the bucket & object level
## Data Consumer Role
- No changes needed for this role specific to Glue crawler
