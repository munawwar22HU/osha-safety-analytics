
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select violation_type_desc
from OSHA_DB.MARTS.mart_safety__violation_types
where violation_type_desc is null



  
  
      
    ) dbt_internal_test