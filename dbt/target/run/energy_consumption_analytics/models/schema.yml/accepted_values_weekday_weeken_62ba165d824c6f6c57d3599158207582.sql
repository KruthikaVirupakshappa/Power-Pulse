select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        weekday_weekend as value_field,
        count(*) as n_records

    from USER_DB_LYNX.analytics.weekday_weekend
    group by weekday_weekend

)

select *
from all_values
where value_field not in (
    'WEEKDAY','WEEKEND'
)



      
    ) dbt_internal_test