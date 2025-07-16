with 

source as (

    select * from {{ source('db_landing', 'src_raw_stock_prices') }}

),

stg_stock_prices as (

    select
        cast(upper(id) as string) as id,
        cast(date as date) as date,
        cast(close as double) as close,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(open as double) as open,
        try_cast(volume as integer) as volume,
        replace(cast(upper(ticker) as string), '-', '.') as ticker,
        cast(adj_close as double) as adj_close,
        current_timestamp() as created_at

    from source

)

select * from stg_stock_prices
