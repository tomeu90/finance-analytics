with 

source as (

    select * from {{ source('db_landing', 'src_raw_sic_codes') }}

),

stg_sic_codes as (

    select
        cast(sic_code as integer) as sic_code,
        cast(upper(office) as string) as office,
        cast(upper(industry_title) as string) as industry_title,
        current_timestamp() as created_at

    from source

)

select * from stg_sic_codes
