## Data Quality & Validation
We use dbt's native testing framework to ensure the integrity of the Star Schema.
- docker exec -it --workdir /usr/app/dbt_project docker-dbt-1 dbt test --profiles-dir .

## Validation Results
Every run is validated against 35+ data tests, including:

Primary Key Integrity: unique and not_null on all surrogate keys.

Relationship Testing: Foreign key validation between Facts and Dimensions.

Business Logic: Domain-specific tests (e.g., ensuring price > 0 and review_score is 1-5).

## Output should be all pass like this:
20:50:05  Found 15 models, 35 data tests, 9 sources, 525 macros
20:50:05  Concurrency: 4 threads (target='dev')
...
20:50:05  Finished running 35 data tests in 0 hours 0 minutes and 36.26 seconds (36.26s).
20:50:05  Completed successfully