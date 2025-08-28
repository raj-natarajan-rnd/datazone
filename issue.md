The console *does* show DDL/DML checkboxes. The catch is: those Lake Formation permissions are **generic** and enforcement depends on the **engine + table type**.

For **Athena + Apache Iceberg**:

* **DDL (CREATE/ALTER/DROP)**
  Even if you grant LF “Create table/Alter/Drop,” **Athena won’t run Iceberg DDL** when the table’s S3 location is **registered with LF**. That’s a hard limitation. Use Glue/EMR (IAM/S3) for DDL, or point DDL at an **unregistered** prefix.

* **DML (INSERT/DELETE/UPDATE/MERGE)**
  Athena can do these on Iceberg, but the **write actually happens to S3**. Lake Formation does **not** enforce the data-plane write; you need **direct IAM/S3 permissions** (Put/Delete/List/Get) on the table’s data and Iceberg metadata prefixes. Granting LF “Insert/Delete” in the UI **isn’t sufficient** by itself for Iceberg writes; think of LF as governing **reads** (SELECT, column/row filters) and catalog-level auth, while **writes are controlled by IAM/S3**.

* **Column-level control**
  LF column/row filters apply to **SELECT** only. There’s no “column-level write” control in LF for Iceberg.

### Practical setup that works

* **Readers:** Grant LF `SELECT` (plus column/row filters as needed).
* **Writers (ETL / admin role):**

  * Give **S3 write** permissions to the Iceberg data & metadata paths.
  * Grant LF **DATA\_LOCATION\_ACCESS** on the bucket/prefix and **DB** permissions (e.g., DESCRIBE/CREATE\_TABLE/ALTER/DROP) so the engine can resolve catalog objects—but do DDL via Glue/EMR or an unregistered prefix.
* Keep “governed reads, IAM writes” as your mental model for Iceberg with LF.


You’re right—those earlier links didn’t work. Here are the **official AWS docs** with the exact points we discussed:

* **Athena + Iceberg: considerations & limits** — *“Athena does not currently support DDL on Iceberg tables registered with Lake Formation.”* ([AWS Docs][1])
* **Athena when querying LF-registered data** — explicit *Iceberg DDL support* note (requires **direct S3** for DDL; otherwise access-denied/timeout). ([AWS Docs][2])
* **Lake Formation data filters** — column/row/cell filters **apply to reads (SELECT) only**. ([AWS Docs][3])
* **LF “which engines & table types are enforced”** — support matrix for Athena, Glue, EMR, Redshift Spectrum across **standard** and **transactional (Iceberg/Hudi/Delta)** tables. ([AWS Docs][4])
* **Hybrid Access Mode (governed reads + IAM/S3 writes)** — official guide. ([AWS Docs][5])
* **Hybrid pattern walkthrough (Iceberg reads via LF, writes via IAM)** — AWS Big Data Blog. ([Amazon Web Services, Inc.][6])
* **EMR + Lake Formation with Iceberg** — support matrix (DML, time travel, etc.). ([AWS Docs][7])

Want me to pull the key sentences into a one-pager or map this to your roles/buckets for a ready-to-apply policy set?

[1]: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html?utm_source=chatgpt.com "Query Apache Iceberg tables - Amazon Athena"
[2]: https://docs.aws.amazon.com/athena/latest/ug/lf-athena-limitations.html?utm_source=chatgpt.com "Considerations and limitations for querying data registered ..."
[3]: https://docs.aws.amazon.com/lake-formation/latest/dg/data-filtering.html?utm_source=chatgpt.com "Data filtering and cell-level security in Lake Formation"
[4]: https://docs.aws.amazon.com/lake-formation/latest/dg/working-with-services.html?utm_source=chatgpt.com "Working with other AWS services - AWS Lake Formation"
[5]: https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-mode.html?utm_source=chatgpt.com "Hybrid access mode - AWS Lake Formation"
[6]: https://aws.amazon.com/blogs/big-data/read-and-write-apache-iceberg-tables-using-aws-lake-formation-hybrid-access-mode/?utm_source=chatgpt.com "Read and write Apache Iceberg tables using AWS Lake ..."
[7]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/iceberg-with-lake-formation.html?utm_source=chatgpt.com "Apache Iceberg and Lake Formation with Amazon EMR"


