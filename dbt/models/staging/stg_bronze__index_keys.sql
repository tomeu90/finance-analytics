with 

source as (

    select * from {{ source('db_landing', 'src_raw_index_keys') }}

),

stg_index_keys as (

    select
        cast(cik as string) as cik,
        cast(upper(ticker) as string) as ticker,
        cast(upper(name) as string) as name,
        cast(upper(exchange) as string) as exchange,
        try_cast(sic as integer) as sic,
        current_timestamp() as created_at
    from source

)

select * from stg_index_keys
