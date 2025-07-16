{{ config(
    materialized = 'incremental',
    unique_key = ['date_session', 'id_cik']
) }}

{% if is_incremental() %}

with max_dates as (
    select
        id_cik,
        max(date_session) as max_date_session
    from {{ this }}
    group by id_cik
),

fct_security_prices as (

    select
        k.cik as id_cik,
        p.ticker as code_symbol,
        p.date as date_session,
        round(coalesce(p.adj_close, p.close), 2) as price_close,
        round(coalesce(p.high, p.close), 2) as price_high,
        round(coalesce(p.low, p.close), 2) as price_low,
        round(coalesce(p.open, p.close), 2) as price_open,
        coalesce(p.volume, 0) as amt_volume,
        current_timestamp() as created_at

    from {{ ref('stg_bronze__stock_prices') }} p
    inner join {{ ref('stg_bronze__index_keys') }} k
        on p.ticker = k.ticker

    left join max_dates m
        on k.cik = m.id_cik

    where p.date > coalesce(m.max_date_session, '1900-01-01')
)

select * from fct_security_prices

{% else %}

with fct_security_prices as (

    select
        k.cik as id_cik,
        p.ticker as code_symbol,
        p.date as date_session,
        round(coalesce(p.adj_close, p.close), 2) as price_close,
        round(coalesce(p.high, p.close), 2) as price_high,
        round(coalesce(p.low, p.close), 2) as price_low,
        round(coalesce(p.open, p.close), 2) as price_open,
        coalesce(p.volume, 0) as amt_volume,
        current_timestamp() as created_at

    from {{ ref('stg_bronze__stock_prices') }} p
    inner join {{ ref('stg_bronze__index_keys') }} k
        on p.ticker = k.ticker

)

select * from fct_security_prices

{% endif %}
