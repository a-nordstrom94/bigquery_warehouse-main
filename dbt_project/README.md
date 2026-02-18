# Olist dbt Transformation Layer
### *Silver & Gold Layer Engineering*

This dbt project handles the transformation of raw Olist e-commerce data into a business-ready dimensional model in Google BigQuery.

---

## Project Structure & Conventions

We follow a modular Medallion-style architecture to ensure clean lineage and maintainability:

| Prefix | Layer | Materialization | Description |
| :--- | :--- | :--- | :--- |
| `stg_` | **Silver** | `view` | Source alignment, type casting, and basic cleaning. |
| `int_` | **Intermediate** | `ephemeral` | Complex joins or pivoted logic (hidden from end-users). |
| `fct_` | **Gold (Fact)** | `incremental` | Quantitative events (Orders, Items). Uses `merge` strategy. |
| `dim_` | **Gold (Dim)** | `table` | Descriptive entities (Customers, Products, Sellers). |

---

## Data Quality Framework
Our testing suite is designed to prevent "Data Drift" and ensure the BI layer remains accurate. 

### **Test Categories**
1.  **Generic Tests:** `unique`, `not_null`, and `relationships` are applied to all primary and foreign keys across the Gold layer.
2.  **Validation Tests:** We utilize `dbt-expectations` to enforce business constraints:
    * `expect_column_values_to_be_between` for review scores (1-5).
    * `expect_column_min_to_be_at_least` for prices and payment values (0).
3.  **Custom Macro Tests:**
    * `test_row_count_match`: A custom audit test to ensure the `Silver -> Gold` transition didn't lose records during filtering.

### **Running Tests**
To execute the full suite and ensure the warehouse is "Healthy":
```bash
docker exec -it docker-dbt-1 bash -c "cd /usr/app/dbt_project && dbt test"