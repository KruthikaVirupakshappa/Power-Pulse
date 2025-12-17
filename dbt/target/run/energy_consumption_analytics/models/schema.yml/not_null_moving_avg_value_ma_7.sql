select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select value_ma_7
from USER_DB_LYNX.analytics.moving_avg
where value_ma_7 is null



      
    ) dbt_internal_test