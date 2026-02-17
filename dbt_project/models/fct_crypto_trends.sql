{{ config(materialized='table') }}

with raw_data as (
    select * from {{ ref('all_prices') }}
),

final as (
    select
        symbol,
        price,
        event_time,
        avg(price) over (
            partition by symbol 
            order by event_time 
            rows between 9 preceding and current row
        ) as moving_avg_10,
        price - lag(price) over (
            partition by symbol 
            order by event_time
        ) as price_diff
    from raw_data
)

select * from final