
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select inspection_id
from OSHA_DB.STAGING.stg_osha__inspections
where inspection_id is null



  
  
      
    ) dbt_internal_test