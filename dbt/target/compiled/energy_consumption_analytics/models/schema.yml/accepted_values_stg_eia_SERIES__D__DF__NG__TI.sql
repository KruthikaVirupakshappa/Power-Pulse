
    
    

with all_values as (

    select
        SERIES as value_field,
        count(*) as n_records

    from USER_DB_LYNX.analytics_staging.stg_eia
    group by SERIES

)

select *
from all_values
where value_field not in (
    'D','DF','NG','TI'
)


