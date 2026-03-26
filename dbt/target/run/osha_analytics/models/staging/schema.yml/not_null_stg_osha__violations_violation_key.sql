
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select violation_key
from OSHA_DB.STAGING.stg_osha__violations
where violation_key is null



  
  
      
    ) dbt_internal_test