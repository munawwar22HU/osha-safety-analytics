
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select inspection_year
from OSHA_DB.MARTS.mart_safety__industry_summary
where inspection_year is null



  
  
      
    ) dbt_internal_test