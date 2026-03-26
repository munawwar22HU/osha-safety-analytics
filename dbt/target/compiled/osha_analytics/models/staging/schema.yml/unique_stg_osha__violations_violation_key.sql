
    
    

select
    violation_key as unique_field,
    count(*) as n_records

from OSHA_DB.STAGING.stg_osha__violations
where violation_key is not null
group by violation_key
having count(*) > 1


