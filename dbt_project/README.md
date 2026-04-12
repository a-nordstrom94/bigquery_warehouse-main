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
| `snap_` | **Snapshots (SCD2)** | `snapshot` | Slowly Changing Dimension Type 2 history (`snap_products`, `snap_sellers`). Tracks changes via `dbt_valid_from` / `dbt_valid_to` and `is_current` flag. |

---

## Data Quality Framework
Our testing suite is designed to prevent "Data Drift" and ensure the BI layer remains accurate. The full suite contains **99 tests** across all layers.

### **Test Categories**
1.  **Generic Tests:** `unique`, `not_null`, and `relationships` are applied to all primary and foreign keys across the Silver and Gold layers.
2.  **Compound Key Tests:** `dbt_utils.unique_combination_of_columns` validates composite natural keys (e.g., `order_id` + `order_item_id` on order items, `order_id` + `payment_sequential` on payments).
3.  **Conditional Uniqueness:** `unique` with `where: "is_current = true"` on SCD2 dimensions (`dim_products`, `dim_sellers`) to correctly validate only current records.
4.  **Accepted Values:** `order_status`, `payment_type`, `review_score`, and `delivery_status` are validated against known enums.
5.  **Validation Tests:** `dbt_expectations.expect_column_values_to_be_between` enforces business constraints on monetary fields (`price`, `freight_value`, `payment_value`) and review scores (1–5).
6.  **Custom Macro Tests:**
    * `test_row_count_match`: A custom audit test to ensure the `Silver -> Gold` transition didn't lose records during filtering.

### **Running Tests**
To execute the full suite and ensure the warehouse is "Healthy":
```bash
docker exec -it docker-dbt-1 bash -c "cd /usr/app/dbt_project && dbt test"
```