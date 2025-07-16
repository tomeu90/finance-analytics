with

source as (

    select * from {{ source('db_landing', 'src_raw_djia_index') }}

),

stg_djia_index as (

    select
        cast(upper(company) as string) as company,
        cast(upper(exchange) as string) as exchange,
        cast(upper(symbol) as string) as symbol,
        cast(upper(industry) as string) as industry,
        cast(date_added as date) as date_added,
        cast(notes as string) as notes,
        cast(index_weighting as string) as index_weighting,
        current_timestamp() as created_at

    from source

)

select * from stg_djia_index

