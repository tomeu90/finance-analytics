
with 

source as (

    select * from {{ source('db_landing', 'src_raw_sp_index') }}

),

stg_sp_index as (

    select
        cast(upper(symbol) as string) as symbol,
        cast(upper(security) as string) as security,
        cast(upper(gics_sector) as string) as gics_sector,
        cast(upper(gics_sub_industry) as string) as gics_sub_industry,
        cast(upper(headquarters_location) as string) as headquarters_location,
        cast(date_added as date) as date_added,
        cast(cik as integer) as cik,
        cast(upper(founded) as string) as founded,
        current_timestamp() as created_at

    from source

)

select * from stg_sp_index
