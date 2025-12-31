{{ config(materialized='table') }}

with bronze_reviews as (
    select * from {{ source('bronze', 'reviews') }}
)

select
    cast(review_id as string) as review_id,
    cast(order_id as string) as order_id,
    cast(review_score as int64) as review_score,
    trim(cast(review_comment_title as string)) as review_comment_title,
    trim(cast(review_comment_message as string)) as review_comment_message,
    timestamp(cast(review_creation_date as string)) as review_creation_date,
    timestamp(cast(review_answer_timestamp as string)) as review_answer_timestamp
from bronze_reviews