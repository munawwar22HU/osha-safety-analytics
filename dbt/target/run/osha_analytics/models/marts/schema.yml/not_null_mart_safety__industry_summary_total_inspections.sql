
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_inspections
from OSHA_DB.MARTS.mart_safety__industry_summary
where total_inspections is null



  
  
      
    ) dbt_internal_test