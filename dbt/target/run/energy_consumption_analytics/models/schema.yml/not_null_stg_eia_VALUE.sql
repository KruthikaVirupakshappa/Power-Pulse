select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select VALUE
from USER_DB_LYNX.analytics_staging.stg_eia
where VALUE is null



      
    ) dbt_internal_test