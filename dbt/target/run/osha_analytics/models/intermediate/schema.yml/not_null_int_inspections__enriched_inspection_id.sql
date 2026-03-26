
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select inspection_id
from OSHA_DB.INTERMEDIATE.int_inspections__enriched
where inspection_id is null



  
  
      
    ) dbt_internal_test