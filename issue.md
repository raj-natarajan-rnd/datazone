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

