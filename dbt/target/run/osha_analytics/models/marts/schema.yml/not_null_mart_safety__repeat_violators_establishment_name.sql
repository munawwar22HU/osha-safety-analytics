
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select establishment_name
from OSHA_DB.MARTS.mart_safety__repeat_violators
where establishment_name is null



  
  
      
    ) dbt_internal_test