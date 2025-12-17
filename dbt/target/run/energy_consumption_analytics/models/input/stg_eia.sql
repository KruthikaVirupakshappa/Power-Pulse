
  create or replace   view USER_DB_LYNX.analytics_staging.stg_eia
  
   as (
    

SELECT 
    REGION,
    DATE,
    SERIES,
    VALUE
FROM USER_DB_MONKEY.staging.RTO_REGION_HOURLY_STAGING
  );

