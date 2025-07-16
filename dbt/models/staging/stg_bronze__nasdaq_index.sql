with 

source as (

    select * from {{ source('db_landing', 'src_raw_nasdaq_index') }}

),

stg_nasdaq_index as (

    select
        cast(upper(ticker) as string) as ticker,
        cast(upper(company) as string) as company,
        cast(upper(gics_sector) as string) as gics_sector,
        cast(upper(gics_sub_industry) as string) as gics_sub_industry,
        current_timestamp() as created_at

    from source

)

select * from stg_nasdaq_index
