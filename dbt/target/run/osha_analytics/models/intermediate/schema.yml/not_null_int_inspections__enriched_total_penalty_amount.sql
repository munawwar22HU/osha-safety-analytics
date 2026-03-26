
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_penalty_amount
from OSHA_DB.INTERMEDIATE.int_inspections__enriched
where total_penalty_amount is null



  
  
      
    ) dbt_internal_test