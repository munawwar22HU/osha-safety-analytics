
    
    

with all_values as (

    select
        state_code as value_field,
        count(*) as n_records

    from OSHA_DB.STAGING.stg_osha__inspections
    group by state_code

)

select *
from all_values
where value_field not in (
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','GU','VI','AS','MP'
)


