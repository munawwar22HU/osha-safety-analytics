
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select state_code
from OSHA_DB.MARTS.mart_safety__state_trends
where state_code is null



  
  
      
    ) dbt_internal_test