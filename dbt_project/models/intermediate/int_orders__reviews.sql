{{
    config(materialized='ephemeral')
}}

with reviews as (
    select * from {{ ref('stg_olist__reviews') }}
),

-- Take the latest review per order (some orders have duplicates)
ranked as (
    select
        *,
        row_number() over (
            partition by order_id 
            order by review_creation_date desc
        ) as rn
    from reviews
),

deduplicated as (
    select
        order_id,
        review_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date as review_created_at,
        review_answer_timestamp as review_answered_at
    from ranked
    where rn = 1
)

select * from deduplicated