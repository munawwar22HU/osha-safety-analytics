
    
    

select
    inspection_id as unique_field,
    count(*) as n_records

from OSHA_DB.INTERMEDIATE.int_inspections__enriched
where inspection_id is not null
group by inspection_id
having count(*) > 1


