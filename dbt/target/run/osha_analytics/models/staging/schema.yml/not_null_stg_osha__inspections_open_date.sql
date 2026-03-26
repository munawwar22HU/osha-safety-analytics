
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select open_date
from OSHA_DB.STAGING.stg_osha__inspections
where open_date is null



  
  
      
    ) dbt_internal_test