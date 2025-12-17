select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select SERIES
from USER_DB_LYNX.analytics.moving_avg
where SERIES is null



      
    ) dbt_internal_test