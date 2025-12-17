
  
    

        create or replace transient table USER_DB_LYNX.analytics.weekday_weekend
         as
        (-- models/output/rto_weekday_weekend_summary.sql


with base as (
    select
        REGION,
        DATE,
        SERIES,
        VALUE
    from USER_DB_LYNX.analytics_staging.stg_eia
),

with_flags as (
    select
        REGION,
        DATE,
        SERIES,
        VALUE,
        case
            when dayofweek(DATE) in (1, 7) then 'WEEKEND'
            else 'WEEKDAY'
        end as weekday_weekend
    from base
)

select
    REGION,
    SERIES,
    weekday_weekend,
    avg(VALUE) as avg_value,
    min(VALUE) as min_value,
    max(VALUE) as max_value
from with_flags
group by REGION, SERIES, weekday_weekend
        );
      
  