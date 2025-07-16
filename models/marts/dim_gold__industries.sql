with 

dim_industries as (

    select 
        0 as code_sic,
        'NOT CLASSIFIED' as desc_office,
        'NOT CLASSIFIED' as desc_industry

    union all

    select  
        sic_code as code_sic,
        office as desc_office,
        industry_title as desc_industry

    from {{ ref('stg_bronze__sic_codes') }}

)

select 
    *,
    current_timestamp() as created_at
    
from dim_industries