with 

source as (

    select * from {{ source('db_landing', 'src_raw_insiders_trx') }}

),

stg_insiders_trx as (

    select
        to_timestamp(filing, 'dd MMM yyyy HH:mm') as filing,
        cast(upper(symbol) as string) as symbol,
        cast(upper(security) as string) as security,
        cast(upper(reporting_name) as string) as reporting_name,
        cast(upper(relationship) as string) as relationship,
        to_date(trans_date, 'dd MMM yyyy') as trans_date,
        cast(upper(purchase_sale) as string) as purchase_sale,
        cast(shares as decimal(15,2)) as shares,
        cast(price as decimal(15,2)) as price,
        cast(amount as decimal(15,2)) as amount,
        cast(upper(d_i) as string) as d_i,
        current_timestamp() as created_at

    from source

)

select * from stg_insiders_trx
