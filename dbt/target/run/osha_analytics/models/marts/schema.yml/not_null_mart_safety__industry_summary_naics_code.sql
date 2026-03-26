
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select naics_code
from OSHA_DB.MARTS.mart_safety__industry_summary
where naics_code is null



  
  
      
    ) dbt_internal_test